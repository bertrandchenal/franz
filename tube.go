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

const MaxBucketSize = int64(4294967296) // 2^32 = 4G
const BaseBucketSize = int64(268435455) // 2^8 * 2^20 = 256M
var newMutex = sync.Mutex{}

type Bucket struct {
	Offset    int64 // start offset (in tube) of the bucket
	Size      int64 // offset + size is end offset wrt the tube
	Timestamp int64
	Name      string
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
		if splitted[1] != "franz" {
			continue
		}
		if bucket_offset, err = strconv.ParseInt(splitted[0], 16, 64); err != nil {
			log.Fatal(err)
		}

		// We read the first timestamp of the index
		idx_file := path.Join(root, splitted[0]) + ".idx"
		idx_fh, err := mmap.Open(idx_file)
		check(err)
		defer idx_fh.Close()
		buff := make([]byte, 4)
		_, err = idx_fh.ReadAt(buff, 4)
		timestamp := int64(binary.BigEndian.Uint32(buff))

		// Instanciate bucket object and append it
		new_bucket := NewBucket(bucket_offset, file.Size(), timestamp)
		buckets = append(buckets, new_bucket)

	}
	return buckets
}

func NewBucket(offset int64, size int64, timestamp int64) *Bucket {
	newMutex.Lock()
	defer newMutex.Unlock()

	name := strconv.FormatInt(offset, 16)
	return &Bucket{offset, size, timestamp, name}
}

func NewTube(root string, name string) *Tube {
	root = path.Join(root, name)
	os.MkdirAll(root, 0750)
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

func (self *Tube) GetBucket(offset int64, timestamp int64) *Bucket {
	var bucket *Bucket
	pos := sort.Search(len(self.buckets), func(i int) bool {
		b := self.buckets[i]
		return b.Offset+b.Size >= offset && b.Timestamp >= timestamp
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

func (self *Tube) TailBucket(chunk_size int64, now int64) *Bucket {
	// Returns the latest bucket of the tube (if is there is still
	// place enough for the chunk size) or create a new one.

	if chunk_size > MaxBucketSize {
		panic("Chunk size bigger that MaxBucketSize") //FIXME return error
	}

	// No bucket yet, create it
	if len(self.buckets) == 0 {
		new_bucket := NewBucket(0, 0, now)
		self.buckets = append(self.buckets, new_bucket)
		return new_bucket
	}

	// Check if the tail bucket has enough place left
	tail_bucket := self.buckets[len(self.buckets)-1]
	if tail_bucket.Size+chunk_size > BaseBucketSize {
		new_bucket := NewBucket(tail_bucket.Offset+tail_bucket.Size, 0, now)
		self.buckets = append(self.buckets, new_bucket)
		return new_bucket
	}
	return tail_bucket
}

func (self *Tube) Append(data []byte, tags ...string) error {
	// Append data to tube and add data offset to the given tag indexes
	self.append_mutex.Lock()
	defer func() {
		self.append_mutex.Unlock()
	}()

	now := time.Now().Unix()
	bucket := self.TailBucket(int64(len(data)*8), now)
	filename := path.Join(self.Root, bucket.Name)
	// Open bucket file
	fh, err := os.OpenFile(filename+".franz", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0650)
	check(err)
	defer fh.Close()

	// Append data to file
	_, err = fh.Write(data)
	check(err)

	// Append file size and timestamp to indexes
	offset_buff := make([]byte, 4) // TODO use explicit type, test if offset fit on 32bit
	timestamp_buff := make([]byte, 4)
	binary.BigEndian.PutUint32(offset_buff, uint32(self.Len))
	binary.BigEndian.PutUint32(timestamp_buff, uint32(now))
	idx_row := append(offset_buff, timestamp_buff...)
	err = self.UpdateIndex(filename, idx_row)
	check(err)

	for _, name := range tags {
		err = self.UpdateIndex(filename+"-"+name, offset_buff) // XXX idx_row ?
		check(err)
	}

	// Keep track of bucket size
	bucket.Size += int64(len(data))
	self.Len += int64(len(data))
	return nil
}

func (self *Tube) UpdateIndex(index_name string, offset []byte) error {
	// Open index file
	fh, err := os.OpenFile(index_name+".idx", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0650)
	check(err)
	defer fh.Close()

	// Append offset
	_, err = fh.Write(offset)
	check(err)
	return nil
}

func (self *Tube) Read(offset int64, timestamp int64, tags ...string) (int64, []byte, error) {
	// Find matching bucket (TODO should take tags into account)
	bucket := self.GetBucket(offset, timestamp)
	if bucket == nil {
		err := fmt.Errorf("No bucket for offset %d in %q", offset, self.Name)
		return 0, nil, err
	}
	relative_offset, chunk_size := self.Search(bucket, offset, timestamp, tags...)
	if chunk_size == 0 {
		next_offset := bucket.Offset + bucket.Size
		return next_offset, nil, nil
	}
	// Read actual content
	filename := path.Join(self.Root, bucket.Name)
	bucket_fh, err := mmap.Open(filename + ".franz")
	check(err)
	defer bucket_fh.Close()

	chunk_content := make([]byte, chunk_size)
	_, err = bucket_fh.ReadAt(chunk_content, relative_offset)
	check(err)

	next_offset := bucket.Offset + relative_offset + chunk_size
	return next_offset, chunk_content, nil
}

func (self *Tube) Search(bucket *Bucket, offset int64, timestamp int64, tags ...string) (int64, int64) {
	// Find the next starting block whose position is bigger or equal
	// to offset and timestamp
	filename := path.Join(self.Root, bucket.Name)
	// offset inside the bucket is relative to the offset of the
	// bucket itself
	relative_offset := offset - bucket.Offset

	// Search for a common offset among given tags (XXX timestamp?)
	for _, tag := range tags {
		tag_idx_fh, err := mmap.Open(filename + "-" + tag + ".idx")
		if os.IsNotExist(err) {
			relative_offset = bucket.Size
			continue
		} else {
			check(err)
		}
		defer tag_idx_fh.Close()

		// Each index item take 2x32bits (4 bytes)
		nb_pos := tag_idx_fh.Len() / 4
		buff := make([]byte, 4)
		pos := sort.Search(nb_pos, func(i int) bool {
			tag_idx_fh.ReadAt(buff, int64(i)*4)
			value := binary.BigEndian.Uint32(buff)
			return int64(value) >= relative_offset
		})

		// Forward offset to first matching position for tag
		if pos < nb_pos {
			tag_idx_fh.ReadAt(buff, int64(pos)*4)
			new_offset := int64(binary.BigEndian.Uint32(buff))
			if new_offset > relative_offset {
				relative_offset = new_offset
			}
		} else {
			// No matching offset in tag file
			relative_offset = bucket.Size
		}
	}

	// Search in the main index to discover block boundary
	idx_fh, err := mmap.Open(filename + ".idx")
	check(err)
	defer idx_fh.Close()

	// Each index item take 64bits (8 bytes), 4 bytes for the offset,
	// 4 for timestamp
	nb_pos := idx_fh.Len() / 8
	buff := make([]byte, 4)
	pos := sort.Search(nb_pos, func(i int) bool {
		_, err = idx_fh.ReadAt(buff, int64(i)*8)
		check(err)
		idx_os := binary.BigEndian.Uint32(buff)

		_, err = idx_fh.ReadAt(buff, int64(i)*8+4)
		check(err)
		idx_ts := binary.BigEndian.Uint32(buff)

		return int64(idx_os) >= relative_offset && int64(idx_ts) >= timestamp
	})

	// pos + 1 tells where the chunk stop
	next_pos := pos + 1
	var chunk_size int64
	if next_pos >= nb_pos {
		// we have reached the last position, this means the requested
		// message span until the end of the bucket
		chunk_size = bucket.Size - relative_offset
	} else {
		_, err = idx_fh.ReadAt(buff, int64(next_pos)*8)
		check(err)
		value := binary.BigEndian.Uint32(buff)
		chunk_size = int64(value) - relative_offset
	}
	return relative_offset, chunk_size
}
