package main

import (
	"errors"
	"io"
	"os"
)

var (
	ErrTooLargePage = errors.New("too large page")
)

const PageSize = 4096

type Page = []byte
type PageId = int64

type DiskManager struct {
	heapFile   *os.File
	nextPageId PageId
}

func New(heapFile *os.File) (*DiskManager, error) {
	fi, err := heapFile.Stat()
	if err != nil {
		return nil, err
	}

	nextPageId := PageId(fi.Size() / PageSize)

	return &DiskManager{
		heapFile:   heapFile,
		nextPageId: nextPageId,
	}, nil
}

func Open(heapFilePath string) (*DiskManager, error) {
	heapFile, err := os.OpenFile(heapFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return New(heapFile)
}

func (dm *DiskManager) AllocatePage() PageId {
	pageId := dm.nextPageId
	dm.nextPageId += 1
	return pageId
}

func (dm *DiskManager) ReadPageData(pageId PageId, page Page) error {
	offset := PageSize * pageId
	dm.heapFile.Seek(offset, io.SeekStart)
	_, err := dm.heapFile.Read(page)
	return err
}

func (dm *DiskManager) WritePageData(pageId PageId, page Page) error {
	if len(page) > PageSize {
		return ErrTooLargePage
	}

	offset := PageSize * pageId
	dm.heapFile.Seek(offset, io.SeekStart)
	_, err := dm.heapFile.Write(page)
	return err
}
