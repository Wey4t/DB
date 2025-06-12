package server

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"syscall"
	"testing"
	"time"

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
	// Test creating a new page
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

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func Test_kv(t *testing.T) {
	fp, err := os.Create("test_kv.txt")
	defer fp.Close()
	db := &KV{Path: "test_kv.txt"}
	err = db.Open()
	defer os.Remove("test_kv.txt")
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}
	db.page.updates = make(map[uint64][]byte)          // simulate some free pages
	db.page.updates[0] = make([]byte, BTREE_PAGE_SIZE) // simulate a master page

	// db.free.Update(db.page.nfree, []uint64{})          // initialize free list

	defer db.Close()
	// Test inserting a key-value pair
	err = db.Set([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	// Test retrieving the value
	val, ok := db.Get([]byte("k1"))

	assert.True(t, !ok)
	db.Get([]byte("ke2"))
	assert.True(t, !ok)
	val, ok = db.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, "value2", string(val), "Expected value 'value1' for key 'key1'")
	db.Set([]byte("ke3"), []byte("value2"))
	val, ok = db.Get([]byte("ke3"))
	assert.True(t, ok, "Expected key 'ke3' to be found")
	assert.Equal(t, string(val), "value2", "Expected value 'value2' for key 'ke3'")

	// change values of ke3 and key1
	err = db.Set([]byte("ke3"), []byte("new_value2"))
	assert.NoError(t, err, "Expected no error when updating key 'ke3'")
	val, ok = db.Get([]byte("ke3"))
	assert.True(t, ok, "Expected key 'ke3' to be found after update")
	assert.True(t, string(val) == "new_value2", "Expected updated value 'new_value2' for key 'ke3'")

	err = db.Set([]byte("key1"), []byte("newkey1_value1"))
	assert.NoError(t, err, "Expected no error when updating key 'key1'")
	val, ok = db.Get([]byte("key1"))
	assert.True(t, ok, "Expected key 'ke3' to be found after update")
	assert.True(t, string(val) == "newkey1_value1", "Expected updated value 'newkey1_value1' for key 'key1'")
	db.Del([]byte("key1"))
	val, ok = db.Get([]byte("key1"))
	assert.True(t, !ok, "Expected key 'key1' to be found after deletion")
	assert.True(t, val == nil, "Expected value to be nil after deletion of key 'key1'")
	db.Del([]byte("ke3"))
	val, ok = db.Get([]byte("ke3"))
	assert.True(t, !ok, "Expected key 'ke3' to be found after deletion")
	assert.True(t, val == nil, "Expected value to be nil after deletion of key 'ke3'")
	// set 10 random keys
	for i := 0; i < 6; i++ {
		key := randomString(12)
		val := randomString(22)
		err = db.Set([]byte(key), []byte(val))
		assert.NoError(t, err, "Expected no error when inserting key-value pair")
		retrievedVal, ok := db.Get([]byte(key))
		assert.True(t, ok, "Expected key to be found after insertion")
		assert.Equal(t, val, string(retrievedVal), "Expected retrieved value to match inserted value")
	}
	// db.debug("After inserting random keys")

	db.Del([]byte("key1"))
}

func Test_KV(t *testing.T) {
	fmt.Println("=== Simple KV Sequential Test ===")

	// Open database
	path := "test.db"
	db := &KV{Path: path}
	fp, err := os.Create(path)
	err = db.Open()
	defer fp.Close()
	defer os.Remove(path)
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}
	db.page.updates = make(map[uint64][]byte)          // simulate some free pages
	db.page.updates[0] = make([]byte, BTREE_PAGE_SIZE) // simulate a master page

	// db.free.Update(db.page.nfree, []uint64{})          // initialize free list

	defer db.Close()
	fmt.Println("\n1. Setting keys...")

	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		log.Printf("Set key1 failed: %v", err)
	} else {
		fmt.Println("✅ Set key1 -> value1")
	}

	err = db.Set([]byte("key2"), []byte("value2"))
	if err != nil {
		log.Printf("Set key2 failed: %v", err)
	} else {
		fmt.Println("✅ Set key2 -> value2")
	}

	err = db.Set([]byte("key3"), []byte("value3"))
	if err != nil {
		log.Printf("Set key3 failed: %v", err)
	} else {
		fmt.Println("✅ Set key3 -> value3")
	}

	// Test 2: Get the keys
	fmt.Println("\n2. Getting keys...")

	val, found := db.Get([]byte("key1"))
	if found {
		fmt.Printf("✅ Get key1 -> %s\n", val)
	} else {
		fmt.Println("❌ Get key1 -> not found")
	}

	val, found = db.Get([]byte("key2"))
	if found {
		fmt.Printf("✅ Get key2 -> %s\n", val)
	} else {
		fmt.Println("❌ Get key2 -> not found")
	}

	val, found = db.Get([]byte("key3"))
	if found {
		fmt.Printf("✅ Get key3 -> %s\n", val)
	} else {
		fmt.Println("❌ Get key3 -> not found")
	}

	// Test 3: Delete some keys
	fmt.Println("\n3. Deleting keys...")

	deleted, err := db.Del([]byte("key2"))
	if err != nil {
		log.Printf("Delete key2 failed: %v", err)
	} else if deleted {
		fmt.Println("✅ Deleted key2")
	} else {
		fmt.Println("❌ Delete key2 -> not found")
	}

	// Test 4: Get after delete
	fmt.Println("\n4. Getting after delete...")

	val, found = db.Get([]byte("key1"))
	if found {
		fmt.Printf("✅ Get key1 -> %s (still exists)\n", val)
	} else {
		fmt.Println("❌ Get key1 -> not found")
	}

	val, found = db.Get([]byte("key2"))
	if found {
		fmt.Printf("❌ Get key2 -> %s (should be deleted!)\n", val)
	} else {
		fmt.Println("✅ Get key2 -> not found (correctly deleted)")
	}

	val, found = db.Get([]byte("key3"))
	if found {
		fmt.Printf("✅ Get key3 -> %s (still exists)\n", val)
	} else {
		fmt.Println("❌ Get key3 -> not found")
	}

	// Test 5: Update existing key
	fmt.Println("\n5. Updating existing key...")

	err = db.Set([]byte("key1"), []byte("updated_value"))
	if err != nil {
		log.Printf("Update key1 failed: %v", err)
	} else {
		fmt.Println("✅ Updated key1")
	}

	val, found = db.Get([]byte("key1"))
	if found {
		fmt.Printf("✅ Get key1 -> %s (updated)\n", val)
	} else {
		fmt.Println("❌ Get key1 -> not found")
	}

	// Test 6: Delete non-existent key
	fmt.Println("\n6. Deleting non-existent key...")

	deleted, err = db.Del([]byte("nonexistent"))
	if err != nil {
		log.Printf("Delete nonexistent failed: %v", err)
	} else if deleted {
		fmt.Println("❌ Delete nonexistent -> returned true (should be false)")
	} else {
		fmt.Println("✅ Delete nonexistent -> correctly returned false")
	}

	fmt.Println("\n=== Test Complete ===")
}
