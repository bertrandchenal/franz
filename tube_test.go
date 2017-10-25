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
		err = os.Remove(filename)
		if err != nil {
			panic(err)
		}
	}
}

func TestInit(t *testing.T) {
	cleanup()
	tube := NewTube(".", TEST_DIR)
	if len(tube.buckets) > 0 {
		t.Error("data dir not clean")
	}
}

func TestAppend(t *testing.T) {
	cleanup()
	tube := NewTube(".", TEST_DIR)
	hello := []byte("hello")
	// Append with no tags
	err := tube.Append(hello)
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

	//append with tags
	world := []byte("world")
	err = tube.Append(world, "ham", "spam")
	if err != nil {
		t.Error(err)
	}
	files, err = ioutil.ReadDir(TEST_DIR)
	if err != nil {
		t.Error(err)
	}
	if len(files) != 4 {
		t.Error("Unexpected number of file")
	}

	// Check that actual content is there
	content, err := tube.Read(0)
	if err != nil {
		panic(err)
	}
	if string(content) != string(hello) {
		t.Error("Unexpected value")
	}

	content, err = tube.Read(int64(len(world)))
	if err != nil {
		panic(err)
	}
	if string(content) != string(world) {
		t.Error("Unexpected value:", string(content))
	}
}
