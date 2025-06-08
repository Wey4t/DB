package server

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"

	. "types"
)

func Test_MmmapInit(t *testing.T) {
	fp, err := os.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer os.Remove("test.txt")
	// Write a dummy page to the file
	if _, err := fp.Write(make([]byte, BTREE_PAGE_SIZE)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}

	// Initialize mmap
	size, chunk, err := mmapInit(fp)
	if err != nil {
		t.Fatalf("mmapInit failed: %v", err)
	}
	defer syscall.Munmap(chunk)

	assert.Equal(t, size, BTREE_PAGE_SIZE, true, "Expected file size to be equal to BTREE_PAGE_SIZE")
	assert.True(t, len(chunk) >= BTREE_PAGE_SIZE)

}

func Test_ExtendMmap(t *testing.T) {
	fp, err := os.Create("test_extend.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer fp.Close()
	defer os.Remove("test_extend.txt")
	// Write a dummy page to the file
	if _, err := fp.Write(make([]byte, BTREE_PAGE_SIZE)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}

	db := &KV{Path: "test_extend.txt"}
	db.fp = fp

	// Initialize mmap
	size, chunk, err := mmapInit(fp)
	if err != nil {
		t.Fatalf("mmapInit failed: %v", err)
	}
	defer syscall.Munmap(chunk)
	db.mmap.file = size
	db.mmap.total = size
	db.mmap.chunks = append(db.mmap.chunks, chunk)

	// Test extending mmap
	err = extendMmap(db, 2)
	if err != nil {
		t.Fatalf("extendMmap failed: %v", err)
	}
	assert.True(t, db.mmap.total >= 2*BTREE_PAGE_SIZE)

}

func Test_pageGet(t *testing.T) {
	fp, err := os.Create("test_page.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer os.Remove("test_page.txt")
	// Write a dummy page to the file
	if _, err := fp.Write(make([]byte, BTREE_PAGE_SIZE)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}

	db := &KV{Path: "test_page.txt"}
	db.fp = fp

	// Initialize mmap
	size, chunk, err := mmapInit(fp)
	if err != nil {
		t.Fatalf("mmapInit failed: %v", err)
	}
	defer syscall.Munmap(chunk)

	db.mmap.file = size
	db.mmap.total = size
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	db.mmap.chunks[0][BTREE_PAGE_SIZE-1] = 0xFF // mark the last byte for testing
	db.mmap.chunks[0][BTREE_PAGE_SIZE] = 0xF0   // mark the first byte for testing
	page := db.pageGet(0)
	assert.Equal(t, len(page), BTREE_PAGE_SIZE, "Expected page length to be BTREE_PAGE_SIZE")
	assert.Equal(t, page[BTREE_PAGE_SIZE-1], byte(0xFF), "Expected last byte of page to be 0xFF")
	assert.Equal(t, db.pageGet(1)[0], byte(0xF0), "Expected first byte of second page to be 0xF0")
}

func Test_MasterLoad(t *testing.T) {
	fp, err := os.Create("test_page.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer os.Remove("test_page.txt")
	// Write a dummy page to the file
	if _, err := fp.Write(make([]byte, BTREE_PAGE_SIZE*14)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}

	db := &KV{Path: "test_page.txt"}
	db.fp = fp

	// Initialize mmap
	size, chunk, err := mmapInit(fp)
	if err != nil {
		t.Fatalf("mmapInit failed: %v", err)
	}
	defer syscall.Munmap(chunk)

	db.mmap.file = size
	db.mmap.total = size
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	root_indx := uint64(12)
	root := db.pageGet(uint64(root_indx))
	copy(root, []byte("root")) // simulate root page data
	master := db.pageGet(uint64(0))
	copy(master[0:16], []byte(DB_SIG))                      // set the signature
	binary.LittleEndian.PutUint64(master[16:24], root_indx) // set page_used to 1
	binary.LittleEndian.PutUint64(master[24:], 13)          // set page_used to 1
	if err := masterLoad(db); err != nil {
		t.Fatalf("masterLoad failed: %v", err)
	}
	assert.Equal(t, db.page.flushed, uint64(13), "Expected flushed page count to be 13") // should have flushed the master page
	assert.Equal(t, db.tree.Root, root_indx, "Expected root index to match root_indx")   // root should be set correctly
	masterStore(db)                                                                      // create a master page
	err = masterLoad(db)
	if err != nil {
		t.Fatalf("masterLoad failed: %v", err)
	}
	assert.Equal(t, db.page.flushed, uint64(13), "Expected flushed page count to be 13") // should have flushed the master page
	assert.Equal(t, db.tree.Root, uint64(12), "Expected root index to be 12")            // r
}

func Test_pageNew(t *testing.T) {
	fp, err := os.Create("test_page.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer os.Remove("test_page.txt")

	db := &KV{Path: "test_page.txt"}
	db.fp = fp

	// Initialize mmap
	size, chunk, err := mmapInit(fp)
	if err != nil {
		t.Fatalf("mmapInit failed: %v", err)
	}
	defer syscall.Munmap(chunk)
	db.mmap.file = size
	db.mmap.total = size
	db.mmap.chunks = append(db.mmap.chunks, chunk)

	// Test creating a new page
	db.pageNew(make([]byte, BTREE_PAGE_SIZE))
	newPage := db.pageNew(make([]byte, BTREE_PAGE_SIZE))
	assert.True(t, newPage == 1)
	assert.True(t, len(db.pageGet(newPage)) == BTREE_PAGE_SIZE)
}

func Test_extenfile(t *testing.T) {
	fp, err := os.Create("test_extendfile.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer os.Remove("test_extendfile.txt")

	db := &KV{Path: "test_extendfile.txt"}
	db.fp = fp
	// Test extending the file
	err = extendFile(db, 2)
	if err != nil {
		t.Fatalf("extendFile failed: %v", err)
	}

	assert.Equal(t, db.mmap.file, 2*BTREE_PAGE_SIZE, "Expected file size to be twice BTREE_PAGE_SIZE")
}

type C struct {
	tree  BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			Get: func(ptr uint64) []byte {
				node := pages[ptr]
				// assert(ok)
				return node
			},
			New: func(node []byte) uint64 {
				// assert(BNode(node).Nbytes() <= BTREE_PAGE_SIZE)
				// ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				// need a pointer management strategy
				// for now, we just use incresting pointers
				// next ptr is the max ptr + 1 in the pages map, go thourgh map
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
			},
			Del: func(ptr uint64) {
				// assert(pages[ptr] != nil)
				// delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}
func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val // reference data
}
func (c *C) delete(key string) (string, bool) {
	val, ok := c.ref[key]
	if !ok {
		return "", false // key not found
	}
	TreeDelete(&c.tree, c.pages[c.tree.Root], []byte(key)) // delete from the tree
	delete(c.ref, key)                                     // remove from reference data
	return val, true
}
func Test_kv(t *testing.T) {
	fp, err := os.Create("test_kv.txt")
	defer fp.Close()
	c := newC()

	c.add("k1", "hi")
	n := BNode(c.tree.Get(1))
	assert.Equal(t, n.Nkeys(), uint16(2), "Expected 1 key in the root node")
	assert.Equal(t, n.GetKey(1), []byte("k1"), "Expected key 'k1' in the root node")
	assert.Equal(t, n.GetVal(1), []byte("hi"), "Expected value 'hi' for key 'k1'")
	// add n more keys using for loop
	for i := 2; i <= 5; i++ {
		key := fmt.Sprintf("ke%d", i)
		val := fmt.Sprintf("val%d", i)
		c.add(key, val)
	}
	c.add("key1", "value1")
	// need to make a b+tree and save to test_kv.txt
	// save c.pages to test_kv.txt and add the master page
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], c.tree.Root)
	// get the
	binary.LittleEndian.PutUint64(data[24:], uint64(len(c.pages))+1)
	// Write the master page to the file
	if _, err := fp.WriteAt(data[:], 0); err != nil {
		t.Fatalf("Failed to write master page: %v", err)
	}
	// Write the pages to the file
	for ptr, node := range c.pages {
		if _, err := fp.WriteAt(node, int64(ptr*BTREE_PAGE_SIZE)); err != nil {
			t.Fatalf("Failed to write page %d: %v", ptr, err)
		}
	}
	db := &KV{Path: "test_kv.txt"}
	err = db.Open()
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}
	defer db.Close()
	// Test inserting a key-value pair
	err = db.Set([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Test retrieving the value
	val, ok := db.Get([]byte("k1"))
	assert.True(t, ok)
	assert.True(t, string(val) == "hi")
	val, ok = db.Get([]byte("ke2"))
	assert.True(t, ok)
	assert.True(t, string(val) == "val2")
	val, ok = db.Get([]byte("key1"))
	assert.True(t, ok)
	assert.True(t, string(val) == "value2", "Expected value 'value1' for key 'key1'")
}
