package server

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/m1gwings/treedrawer/tree"

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
	_, err := os.Create("test_page.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer os.Remove("test_page.txt")

	db := &KV{Path: "test_page.txt"}
	// Initialize mmap
	db.Open()
	db.page.updates[0] = make([]byte, BTREE_PAGE_SIZE) // simulate some free pages
	// Test creating a new page
	db.pageNew(make([]byte, BTREE_PAGE_SIZE))
	newPage := db.pageNew(make([]byte, BTREE_PAGE_SIZE))
	assert.True(t, newPage == 3)
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
			Get: func(ptr uint64) BNode {
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

func Bnode_to_string(b BNode, id uint64) string {
	if len(b) == 0 {
		return "(empty)"
	}
	var str string
	str += fmt.Sprintf("(%d)", id)
	for i := uint16(0); i < b.Nkeys(); i++ {
		str += fmt.Sprintf("%s:'%s'| ", b.GetKey(i), b.GetVal(i))
	}
	return str
}
func Print_Btree(b_node *BNode, c *KV, parent *tree.Tree, id uint64) {
	if *b_node == nil || b_node.Nkeys() == 0 {
		return
	}
	parent.AddChild(tree.NodeString(Bnode_to_string(*b_node, id)))
	new_tree := parent.Children()[len(parent.Children())-1]
	for i := uint16(0); i < b_node.Nkeys(); i++ {
		b_node_child := BNode(c.page.updates[b_node.GetPtr(i)])
		Print_Btree(&b_node_child, c, new_tree, b_node.GetPtr(i))
	}
}

func (c *KV) debug(log string) {
	fmt.Println("Debug:", log)
	f := tree.NewTree(tree.NodeString("BTree Root"))
	a := BNode(c.page.updates[c.tree.Root])
	Print_Btree(&a, c, f, c.tree.Root)
	fmt.Println(f)
}
func Test_kv(t *testing.T) {
	fp, err := os.Create("test_kv.txt")
	defer fp.Close()
	db := &KV{Path: "test_kv.txt"}
	err = db.Open()
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}
	db.page.updates[0] = make([]byte, BTREE_PAGE_SIZE) // simulate some free pages

	defer db.Close()
	// Test inserting a key-value pair
	err = db.Set([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	err = db.Set([]byte("key1"), []byte("value2"))

	// Test retrieving the value
	val, ok := db.Get([]byte("key1"))
	assert.True(t, ok)
	assert.True(t, string(val) == "value2")
	// Test retrieving another value
	val, ok = db.Get([]byte("key1"))
	assert.True(t, ok)
	assert.True(t, string(val) == "value2", "Expected value 'value2' for key 'key2'")
	// Test retrieving a non-existent key
	val, ok = db.Get([]byte("nonexistent"))
	assert.False(t, ok, "Expected key 'nonexistent' to not exist")
	assert.Nil(t, val, "Expected value for non-existent key to be nil")
	// Test deleting a key
	val, ok = db.Get([]byte("key1"))
	assert.True(t, ok, "Expected key 'key1' to exist before deletion")
	err = db.Set([]byte("key3"), []byte("value3"))
	err = db.Set([]byte("key4"), []byte("value3"))
	err = db.Set([]byte("key5"), []byte("value3"))
	err = db.Set([]byte("ke-3"), []byte("value3"))
	db.debug("ad")
	// Verify the updated value
}
