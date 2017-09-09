package franz

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

const TEST_DIR = "test-dir"

func cleanup() {
	files, err := ioutil.ReadDir(TEST_DIR)
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		filename := path.Join(TEST_DIR, file.Name())
		os.Remove(filename)
	}
}

func TestInit(t *testing.T) {
	cleanup()
	tube := NewTube(TEST_DIR)
	if len(tube.buckets) > 0 {
		t.Error("data dir not clean")
	}
}

func TestAppend(t *testing.T) {
	cleanup()
	tube := NewTube(TEST_DIR)
	err := tube.Append([]byte("hello"))
	if err != nil {
		t.Error(err)
	}
	files, err := ioutil.ReadDir(TEST_DIR)
	if err != nil {
		t.Error(err)
	}

	if len(files) != 2 {
		t.Error("Unexpected number of file")
	}

	if files[0].Size() != 5 {
		t.Error("Unexpected file size")
	}

}
