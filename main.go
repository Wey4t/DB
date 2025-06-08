package main

import (
	"fmt"
	. "types"
)

func main() {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.SetHeader(BNODE_LEAF, 2)
	fmt.Println(node)
}
