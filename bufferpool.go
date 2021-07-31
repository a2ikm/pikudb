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

func (p *BufferPool) size() int {
	return len(p.buffers)
}

func (p *BufferPool) evict() (bufferId, bool) {
	size := p.size()
	pinned := 0

	for {
		buffer := p.buffers[p.nextVictimId]

		if buffer.retainCount == 0 {
			return p.nextVictimId, true
		} else {
			pinned += 1
			if pinned >= size {
				return 0, false
			}
		}

		p.nextVictimId = p.incrementId(p.nextVictimId)
	}
}

func (p *BufferPool) incrementId(bid bufferId) bufferId {
	return (bid + 1) % bufferId(p.size())
}

type BufferPoolManager struct {
	disk      *DiskManager
	pool      *BufferPool
	pageTable map[PageId]bufferId
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
	return buffer, nil
}
