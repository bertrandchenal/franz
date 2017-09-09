package franz

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type Tube struct {
	Root          string
	buckets       int64arr
	MaxBucketSize int64
}

// func intToHex(i int64) {
// 	val, err := fmt.Sprintf("%016x", i)
// 	if err != nil {

// 	}
// }

// func hexToInt(s string) {
// 	return fmt.Sprintf("%016x", i)
// }

func NewTube(root string) *Tube {
	// TODO
	// - create directory, blob & index files if none found
	// - create metadata file (with list of buckets their start time,
	//   start offset, and some descriptions
	// - instanciate mmap

	os.MkdirAll(root, 0750)
	files, err := ioutil.ReadDir(root)
	if err != nil {
		log.Fatal(err)
	}

	var buckets int64arr
	var bucket_id int64
	for _, file := range files {
		splitted := strings.Split(file.Name(), ".")
		if len(splitted) != 2 {
			continue
		}
		if splitted[1] == "franz" {
			if bucket_id, err = strconv.ParseInt(splitted[0], 16, 64); err != nil {
				log.Fatal(err)
			}
			buckets = append(buckets, bucket_id)
		}
	}

	sort.Sort(buckets)
	tube := &Tube{
		Root:    root,
		buckets: buckets,
	}
	return tube
}

func (self *Tube) GetBucket(offset int64) string {
	var bucket_id int64
	if len(self.buckets) == 0 {
		bucket_id = 0
	} else if offset < 0 {
		bucket_id = self.buckets[len(self.buckets)-1]
	} else {
		panic("TODO")
	}
	return fmt.Sprintf("%016x", bucket_id)
}

func (self *Tube) Append(data []byte) error {
	bucket_name := self.GetBucket(-1)
	filename := path.Join(self.Root, bucket_name)
	// Append data to bucket file
	fh, err := os.OpenFile(filename+".franz", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0650)
	info, err := fh.Stat()
	if err != nil {
		return err
	}
	offset := info.Size()

	if err != nil {
		return err
	}
	_, err = fh.Write(data)
	if err != nil {
		return err
	}
	err = fh.Close()
	if err != nil {
		return err
	}

	// Append file size to offset file
	fh, err = os.OpenFile(filename+".idx", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0650)
	if err != nil {
		return err
	}
	buff := make([]byte, 8)
	binary.LittleEndian.PutUint64(buff, uint64(offset))
	_, err = fh.Write(buff)
	if err != nil {
		return err
	}
	err = fh.Close()
	return err
}

// func (self *Tube) Read(offset int64) ([]byte, error) {
// }

// func (self *Tube) Info() ?? {
// }

type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }
