package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_bnode(t *testing.T) {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.SetHeader(BNODE_LEAF, 2)

	if node.Ntype() != BNODE_LEAF {
		t.Errorf("Expected node type %d, got %d", BNODE_LEAF, node.Ntype())
	}

	if node.Nkeys() != 2 {
		t.Errorf("Expected 2 keys, got %d", node.Nkeys())
	}

	// Test setting and getting pointers
	node.SetPtr(0, 12345)
	if ptr := node.GetPtr(0); ptr != 12345 {
		t.Errorf("Expected pointer 12345, got %d", ptr)
	}

	node.SetPtr(1, 67890)
	if ptr := node.GetPtr(1); ptr != 67890 {
		t.Errorf("Expected pointer 67890, got %d", ptr)
	}
	old := BNode(make([]byte, BTREE_PAGE_SIZE))
	old.SetHeader(BNODE_LEAF, 2)
	//             ^type       ^ number of keys
	nodeAppendKV(old, 0, 0, []byte("k1"), []byte("hi"))
	//                 ^ 1st KV
	nodeAppendKV(old, 1, 0, []byte("k3"), []byte("hello"))
	//                 ^ 2nd KV
	assert.Equal(t, old.Nkeys(), uint16(2), "Expected 2 keys in the node")
	assert.Equal(t, old.GetKey(0), []byte("k1"), "Expected key 'k1' in the node")
	assert.Equal(t, old.GetVal(0), []byte("hi"), "Expected value 'hi' for key 'k1'")
	assert.Equal(t, old.GetKey(1), []byte("k3"), "Expected key 'k3' in the node")
	assert.Equal(t, old.GetVal(1), []byte("hello"), "Expected value 'hello' for key 'k3'")
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	new.SetHeader(BNODE_LEAF, 3)
	nodeAppendKV(new, 0, 0, old.GetKey(0), old.GetVal(0))
	assert.Equal(t, new.GetKey(0), []byte("k1"), "Expected key 'k1' in the new node")

	nodeAppendKV(new, 1, 0, []byte("k2"), []byte("b"))
	nodeAppendKV(new, 2, 0, old.GetKey(1), old.GetVal(1))
	assert.Equal(t, new.Nkeys(), uint16(3), "Expected 3 keys in the new node")
	assert.Equal(t, new.GetVal(0), []byte("hi"), "Expected value 'hi' for key 'k1'")
	assert.Equal(t, new.GetKey(1), []byte("k2"), "Expected key 'k2' in the new node")
	assert.Equal(t, new.GetVal(1), []byte("b"), "Expected value 'b' for key 'k2'")
	assert.Equal(t, new.GetKey(2), []byte("k3"), "Expected key 'k3' in the new node")
	assert.Equal(t, new.GetVal(2), []byte("hello"), "Expected value 'hello' for key 'k3'")
	assert.Equal(t, new.Nbytes(), uint16(0x3c), "Expected Nbytes ")
}
