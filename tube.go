package franz

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/exp/mmap"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MaxBucketSize = int64(4294967294) // 2^32-1

type Bucket struct {
	Offset int64 // start offset (in tube) of the bucket
	Size   int64 // offset + size is end offset wrt the tube
}

type BucketList []*Bucket

type Tube struct {
	Name         string
	Root         string
	Len          int64
	buckets      BucketList
	append_mutex *sync.Mutex
}

// Makes BucketList sortable
func (buckets BucketList) Len() int {
	return len(buckets)
}
func (buckets BucketList) Swap(i, j int) {
	buckets[i], buckets[j] = buckets[j], buckets[i]
}
func (buckets BucketList) Less(i, j int) bool {
	return buckets[i].Offset < buckets[j].Offset
}

// Loop on all files of the given directory and detect buckets
func ScanBuckets(root string) BucketList {
	os.MkdirAll(root, 0750)
	files, err := ioutil.ReadDir(root)
	if err != nil {
		log.Fatal(err)
	}

	var buckets BucketList
	var bucket_offset int64
	for _, file := range files {
		splitted := strings.Split(file.Name(), ".")
		if len(splitted) != 2 {
			continue
		}
		if splitted[1] == "franz" {
			if bucket_offset, err = strconv.ParseInt(splitted[0], 16, 64); err != nil {
				log.Fatal(err)
			}
			buckets = append(buckets, &Bucket{bucket_offset, file.Size()})
		}
	}
	return buckets
}

func NewTube(root string, name string) *Tube {
	root = path.Join(root, name)
	buckets := ScanBuckets(root)
	tube_len := MaxOffset(buckets)
	sort.Sort(buckets)

	return &Tube{
		Name:         name,
		Root:         root,
		Len:          tube_len,
		buckets:      buckets,
		append_mutex: &sync.Mutex{},
	}
}

func (self *Tube) GetBucket(offset int64) *Bucket {
	var bucket *Bucket
	pos := sort.Search(len(self.buckets), func(i int) bool {
		b := self.buckets[i]
		return b.Offset+b.Size >= offset
	})

	if pos >= len(self.buckets) {
		return nil
	}
	bucket = self.buckets[pos]
	return bucket
}

func MaxOffset(buckets []*Bucket) int64 {
	if len(buckets) == 0 {
		return 0
	}
	tail_bucket := buckets[len(buckets)-1]
	return tail_bucket.Offset + tail_bucket.Size
}

func (self *Tube) TailBucket(chunk_size int64) *Bucket {
	if chunk_size > MaxBucketSize {
		panic("Chunk size bigger that MaxBucketSize")
	}

	// No bucket yet, create it
	if len(self.buckets) == 0 {
		new_bucket := &Bucket{0, 0}
		self.buckets = append(self.buckets, new_bucket)
		return new_bucket
	}

	// Check if the tail bucket has enough place left for the chunk
	// size
	tail_bucket := self.buckets[len(self.buckets)-1]
	if tail_bucket.Size+chunk_size > MaxBucketSize {
		new_bucket := &Bucket{
			Offset: tail_bucket.Offset + tail_bucket.Size,
			Size:   0,
		}
		self.buckets = append(self.buckets, new_bucket)
		return new_bucket
	}
	return tail_bucket
}

func (self *Tube) Append(data []byte, extra_indexes ...string) error {
	self.append_mutex.Lock()
	defer func() {
		self.append_mutex.Unlock()
	}()

	bucket := self.TailBucket(int64(len(data) * 8))
	bucket_name := strconv.FormatInt(bucket.Offset, 16)
	filename := path.Join(self.Root, bucket_name)
	// Open bucket file
	fh, err := os.OpenFile(filename+".franz", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0650)

	// Append data to file
	_, err = fh.Write(data)
	if err != nil {
		return err
	}
	err = fh.Close()
	if err != nil {
		return err
	}

	// Append file size and timestamp to indexes
	offset_buff := make([]byte, 4) // TODO use explicit type, test if offset fit on 32bit
	timestamp_buff := make([]byte, 4)
	binary.LittleEndian.PutUint32(offset_buff, uint32(self.Len))
	binary.LittleEndian.PutUint32(timestamp_buff, uint32(time.Now().Unix()))
	idx_row := append(offset_buff, timestamp_buff...)
	err = self.UpdateIndex(filename, idx_row)
	if err != nil {
		return err
	}
	for _, idx := range extra_indexes {
		err = self.UpdateIndex(filename+"-"+idx, offset_buff) // XXX idx_row ?
		if err != nil {
			return err
		}
	}

	// Keep track of bucket size
	bucket.Size += int64(len(data))
	self.Len += int64(len(data))
	return nil
}

func (self *Tube) UpdateIndex(index_name string, offset []byte) error {
	// Open index file
	fh, err := os.OpenFile(index_name+".idx", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0650)
	if err != nil {
		return err
	}
	// Append offset
	_, err = fh.Write(offset)
	if err != nil {
		return err
	}
	err = fh.Close()
	if err != nil {
		return err
	}
	return nil
}

func (self *Tube) Read(offset int64) ([]byte, error) {
	// Find matching bucket
	bucket := self.GetBucket(offset)
	if bucket == nil {
		err := fmt.Errorf("Not bucket for offset %d in %q", offset, self.Name)
		return nil, err
	}

	bucket_name := strconv.FormatInt(bucket.Offset, 16)
	filename := path.Join(self.Root, bucket_name)
	idx_fh, err := mmap.Open(filename + ".idx")
	if err != nil {
		return nil, err
	}

	// offset inside the bucket is relative to the offset of the
	// bucket itself
	relative_offset := int32(offset - bucket.Offset)
	// Each index item take 64bits (8 bytes)
	nb_pos := idx_fh.Len() / 8
	buff := make([]byte, 4)
	pos := sort.Search(nb_pos, func(i int) bool {
		idx_fh.ReadAt(buff, int64(i)*8)
		value := binary.LittleEndian.Uint32(buff)
		return int32(value) >= relative_offset
	})

	// pos contains the position in the index file of the requested
	// offset
	idx_fh.ReadAt(buff, int64(pos)*8)
	value := binary.LittleEndian.Uint32(buff)
	if int32(value) != relative_offset {
		err = fmt.Errorf("Offset %q does not exists in %q", offset, self.Name)
		return nil, err
	}

	// pos + 1 tells where the chunk stop
	next_pos := pos + 1
	var chunk_size int32
	if next_pos == nb_pos {
		// we have reached the last position, this means the requested
		// message span until the end of the bucket
		chunk_size = int32(self.Len - offset)
	} else {
		_, err = idx_fh.ReadAt(buff, int64(next_pos)*8)
		if err != nil {
			return nil, err
		}
		value = binary.LittleEndian.Uint32(buff)
		chunk_size = int32(value) - relative_offset
	}

	// Read actual content
	bucket_fh, err := mmap.Open(filename + ".franz")
	if err != nil {
		return nil, err
	}
	chunk_content := make([]byte, chunk_size)
	_, err = bucket_fh.ReadAt(chunk_content, int64(relative_offset))
	if err != nil {
		return nil, err
	}
	return chunk_content, nil
}
