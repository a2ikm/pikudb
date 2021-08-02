package main_test

import (
	"os"
	"testing"

	pikudb "github.com/a2ikm/pikudb"
)

func TestBufferPool(t *testing.T) {
	path := "tmp/bufferpool_test.db"
	_ = os.Remove(path)

	disk, err := pikudb.OpenDiskManager(path)
	if err != nil {
		t.Fatalf("Can't open file: %v\n", err)
	}

	pageId0 := disk.AllocatePage()
	pageId1 := disk.AllocatePage()
	pageId2 := disk.AllocatePage()

	pool := pikudb.NewBufferPool(2)

	m := pikudb.NewBufferPoolManager(disk, pool)

	_, err = m.FetchPage(pageId0)
	if err != nil {
		t.Errorf("Expected FetchData(pageId0) successful but got error: %v\n", err)
	}

	buffer1, err := m.FetchPage(pageId1)
	if err != nil {
		t.Errorf("Expected FetchData(pageId0) successful but got error: %v\n", err)
	}

	_, err = m.FetchPage(pageId2)
	if err == nil {
		t.Errorf("Expected FetchData(pageId2) failure but succeeded\n")
	}

	buffer1.Release()

	_, err = m.FetchPage(pageId2)
	if err != nil {
		t.Errorf("Expected FetchData(pageId2) successful but got error: %v\n", err)
	}
}
