package types

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/m1gwings/treedrawer/tree"
	"github.com/stretchr/testify/assert"
)

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
	c.pages[c.tree.Root] = TreeDelete(&c.tree, c.pages[c.tree.Root], []byte(key)) // delete from the tree
	delete(c.ref, key)                                                            // remove from reference data
	return val, true
}
func print_pages(page BNode, pages *map[uint64]BNode, t *testing.T, p uint64) {
	// recurse through the pages and print them,start from page Bnode,
	// we can use page.GetPtr(i) to get the child nodes
	// base case: if the page is empty, return
	if page.Nkeys() == 0 {
		t.Logf("Page (empty)")
		return
	}
	t.Logf("\nPage %d: (type=%d, keys=%d)", p, page.Ntype(), page.Nkeys())
	for i := uint16(0); i < page.Nkeys(); i++ {
		t.Logf("  Key %d: '%s' -> '%s'", i, page.GetKey(i), page.GetVal(i))
	}
	for j := uint16(0); j < page.Nkeys(); j++ {
		t.Logf("  Ptr %d: %d", j, page.GetPtr(j))
	}
	for i := uint16(0); i < page.Nkeys(); i++ {

		ptr := page.GetPtr(i)
		if ptr == 0 {
			continue
		}
		if n, ok := (*pages)[ptr]; ok {
			// log the children pointer and offset
			print_pages(n, pages, t, ptr) // recurse into child nodes
		} else {
			t.Logf("Page %d: (not found)", ptr)
		}
	}
}

func Bnode_to_string(b BNode, id uint64) string {
	if len(b) == 0 {
		return "(empty)"
	}
	var str string
	str += fmt.Sprintf("(%d)", id)
	for i := uint16(0); i < b.Nkeys(); i++ {
		str += fmt.Sprintf("%s:| ", b.GetKey(i))
	}
	return str
}
func Print_Btree(b_node *BNode, c *C, parent *tree.Tree, id uint64) {
	if len(*b_node) == 0 || b_node.Nkeys() == 0 {
		return
	}
	parent.AddChild(tree.NodeString(Bnode_to_string(*b_node, id)))
	new_tree := parent.Children()[len(parent.Children())-1]
	for i := uint16(0); i < b_node.Nkeys(); i++ {
		b_node_child := c.pages[b_node.GetPtr(i)]
		Print_Btree(&b_node_child, c, new_tree, b_node.GetPtr(i))
	}
}

func (c *C) debug(log string) {
	fmt.Println("Debug:", log)
	f := tree.NewTree(tree.NodeString("BTree Root"))
	a := c.pages[c.tree.Root]
	Print_Btree(&a, c, f, c.tree.Root)
	fmt.Println(f)
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
func randomInt(min, max int) int {
	return rand.Intn(max-min+1) + min
}
func Test_btree(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	c := newC()

	// for i := 0; i <= 5; i++ {
	// 	key := fmt.Sprintf("k%d", i)
	// 	// val is n length string, a very long string
	// 	// make a very long string
	// 	val := fmt.Sprintf("bbbd%d", i)
	// 	c.add(key, val)
	// }
	// c.add("ka", "bbbd0")
	// c.add("kb", "bbbd0")
	// c.add("kc", "bbbd0")
	// c.add("kd", "bbbd0")
	// c.add("ke", "bbbd0")
	// c.add("kf", "bbbd0")
	// c.add("kg", "bbbd0")
	keys := [][]string{}
	for i := 0; i <= 10; i++ {
		key := randomString(4)
		val := randomString(4) // make a very long string
		keys = append(keys, []string{key, val})
		c.add(key, val)
	}
	for i := 0; i <= 10; i++ {
		choose := randomInt(0, len(keys)-1)
		key, _val := keys[choose][0], keys[choose][1]
		// remove the key from the slice to avoid duplicates
		keys = append(keys[:choose], keys[choose+1:]...)
		val, ok := c.delete(key)
		assert.True(t, ok, "Failed to delete key %s", key)
		assert.Equal(t, val, _val, "Deleted value does not match reference value")
	}
	for i := 0; i <= 10; i++ {
		key := randomString(4)
		val := randomString(4) // make a very long string
		keys = append(keys, []string{key, val})

		c.add(key, val)
	}
	for i := 0; i <= 10; i++ {
		choose := randomInt(0, len(keys)-1)
		key, _val := keys[choose][0], keys[choose][1]
		// remove the key from the slice to avoid duplicates
		keys = append(keys[:choose], keys[choose+1:]...)
		val, ok := c.delete(key)
		assert.True(t, ok, "Failed to delete key %s", key)
		assert.Equal(t, val, _val, "Deleted value does not match reference value")
	}

}
