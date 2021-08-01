package main

import (
	"errors"
)

var (
	ErrNoFreeBuffer = errors.New("no free buffer")
)

type Buffer struct {
	PageId      int64
	Page        Page
	isDirty     bool
	retainCount uint64
}

func (b *Buffer) Retain() {
	b.retainCount += 1
}

func (b *Buffer) Release() {
	b.retainCount -= 1
}

type bufferId = uint64

type BufferPool struct {
	buffers      []*Buffer
	nextVictimId bufferId
}

func NewBufferPool(size uint64) *BufferPool {
	return &BufferPool{
		buffers:      make([]*Buffer, size),
		nextVictimId: 0,
	}
}

func (p *BufferPool) size() int {
	return len(p.buffers)
}

func (p *BufferPool) evict() (bufferId, bool) {
	size := p.size()
	pinned := 0

	for {
		buffer := p.buffers[p.nextVictimId]

		if buffer == nil || buffer.retainCount == 0 {
			bufferId := p.incrementVictimId()
			return bufferId, true
		} else {
			pinned += 1
			if pinned >= size {
				return 0, false
			}
		}

		p.incrementVictimId()
	}
}

func (p *BufferPool) incrementVictimId() bufferId {
	bid := p.nextVictimId
	p.nextVictimId = (p.nextVictimId + 1) % bufferId(p.size())
	return bid
}

type BufferPoolManager struct {
	disk      *DiskManager
	pool      *BufferPool
	pageTable map[PageId]bufferId
}

func NewBufferPoolManager(disk *DiskManager, pool *BufferPool) *BufferPoolManager {
	return &BufferPoolManager{
		disk:      disk,
		pool:      pool,
		pageTable: map[PageId]bufferId{},
	}
}

func (m *BufferPoolManager) FetchPage(pageId PageId) (*Buffer, error) {
	bufferId, ok := m.pageTable[pageId]
	if ok {
		buffer := m.pool.buffers[bufferId]
		buffer.Retain()
		return buffer, nil
	}

	bufferId, ok = m.pool.evict()
	if !ok {
		return nil, ErrNoFreeBuffer
	}

	buffer := m.pool.buffers[bufferId]
	if buffer != nil {
		if buffer.isDirty {
			m.disk.WritePageData(buffer.PageId, []byte(buffer.Page))
		}
		delete(m.pageTable, buffer.PageId)
	}

	buffer = &Buffer{
		PageId:      pageId,
		Page:        make([]byte, PageSize),
		isDirty:     false,
		retainCount: 1,
	}
	m.disk.ReadPageData(pageId, buffer.Page)
	m.pageTable[pageId] = bufferId
	m.pool.buffers[bufferId] = buffer
	return buffer, nil
}
