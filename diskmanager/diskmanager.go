package diskmanager

import (
	"io"
	"os"
)

const PageSize = 4096

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

func (dm *DiskManager) ReadPageData(pageId PageId, data []byte) error {
	offset := PageSize * pageId
	dm.heapFile.Seek(offset, io.SeekStart)
	_, err := dm.heapFile.Read(data)
	return err
}

func (dm *DiskManager) WritePageData(pageId PageId, data []byte) error {
	offset := PageSize * pageId
	dm.heapFile.Seek(offset, io.SeekStart)
	_, err := dm.heapFile.Write(data)
	return err
}
