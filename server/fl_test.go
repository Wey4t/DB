package server

import (
	"testing"
	. "types"

	"github.com/stretchr/testify/assert"
)

type T struct {
	fl     *FreeList
	pages  map[uint64]BNode // in-memory pages
	update map[uint64]BNode // for testing update
}

func newT() *T {
	pages := map[uint64]BNode{}
	return &T{
		fl: &FreeList{
			get: func(a uint64) BNode { return pages[a] }, // Mock get function
			new: func(node BNode) uint64 {
				var ptr uint64 = 0
				for p := range pages {
					if p > ptr {
						ptr = p
					}
				}
				ptr++ // next available pointer
				// assert(pages[ptr] == nil)

				pages[ptr] = node
				return ptr
			}, // Mock new function
			use: func(ptr uint64, node BNode) {
				// assert(pages[ptr] != nil)
				pages[ptr] = node // reuse the page
			}, // Mock use function
			head: 0,
		},
		pages: pages,
	}
}

func Test_push(t *testing.T) {
	c := newT()
	c.fl.head = c.fl.new(make([]byte, BTREE_PAGE_SIZE)) // Initialize head with a new page
	flPush(c.fl, []uint64{1, 2, 3, 4, 5}, []uint64{})
	assert.Equal(t, c.fl.Total(), 5, "Expected total to be 5 after pushing 5 items")
}

func Test_update(t *testing.T) {
	c := newT()
	c.fl.head = c.fl.new(make([]byte, BTREE_PAGE_SIZE)) // Initialize head with a new page
}
