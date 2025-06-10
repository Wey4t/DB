package types

import (
	"fmt"
	"testing"

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
				delete(pages, ptr)
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
		str += fmt.Sprintf("%s:'%s'| ", b.GetKey(i), b.GetVal(i))
	}
	return str
}
func Print_Btree(b_node *BNode, c *C, parent *tree.Tree, id uint64) {
	if *b_node == nil || b_node.Nkeys() == 0 {
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
func Test_btree(t *testing.T) {
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
	c.delete("k1")
	c.debug("k1")

	c.delete("k2")
	c.debug("k2")

	c.delete("k3")
	c.debug("k3")
	c.delete("k4")
	c.delete("k5")
	c.debug("all")
	for i := 0; i <= 5; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("val%d", i)
		c.add(key, val)
	}
	c.debug("100")
	fmt.Println(c.ref)
	c.delete("k3")
	c.delete("k4")
	c.delete("k5")
	c.debug("after delete k3, k4, k5")
}
