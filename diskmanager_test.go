package main_test

import (
	"os"
	"testing"

	pikudb "github.com/a2ikm/pikudb"
)

func TestReadAndWrite(t *testing.T) {
	path := "tmp/diskmanager_test.db"
	_ = os.Remove(path)

	m, err := pikudb.OpenDiskManager(path)
	if err != nil {
		t.Fatalf("Can't open file: %v\n", err)
	}

	pageId := m.AllocatePage()

	page := make(pikudb.Page, pikudb.PageSize)
	for i := 0; i < len(page); i += 1 {
		page[i] = byte(i % 256)
	}

	err = m.WritePageData(pageId, page)
	if err != nil {
		t.Fatalf("Can't write page: %v\n", err)
	}

	readPage := make(pikudb.Page, pikudb.PageSize)
	err = m.ReadPageData(pageId, readPage)
	if err != nil {
		t.Fatalf("Can't read page: %v\n", err)
	}

	for i := 0; i < len(page); i += 1 {
		if page[i] != readPage[i] {
			t.Errorf("Read data is not equal to written data at %d\n", i)
		}
	}
}
