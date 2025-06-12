package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"
	. "utils"

	"github.com/stretchr/testify/assert"
)

func TestNext(t *testing.T) {
	// make a tree
	c := newC()
	rand.Seed(time.Now().UnixNano())

	for i := 0; i <= 101; i++ {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(i))
		val := randomString(2000) // make a very long string
		c.tree.Insert(buf[:], []byte(val))
		c.ref[fmt.Sprint(i)] = val // reference data
	}
	// c.debug("")
	biter := BIter{
		tree: &c.tree,
		path: make([]BNode, 0),
		pos:  make([]uint16, 0),
	}
	biter.Init()
	assert.True(t, biter.Valid())
	// there is dummy key in tree
	for i := 0; i <= 55; i++ {
		biter.Next()
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(i))
		k, _ := biter.Deref()
		assert.Equal(t, buf, k)
	}
	//test
	for i := 55; i <= 0; i++ {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(i))
		k, _ := biter.Deref()
		assert.Equal(t, buf, k)
		biter.Prev()
	}
}
func TestSeekLE(t *testing.T) {
	c := newC()

	for i := 0; i <= 1011; i++ {
		a := RandRange(uint32(20), uint32(1000))
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, a)
		val := randomString(2000) // make a very long string
		c.tree.Insert(buf, []byte(val))
		c.ref[fmt.Sprint(i)] = val // reference data
	}
	for i := 0; i <= 1011; i++ {
		a := RandRange(uint32(20), uint32(1000))
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, a)
		biter := c.tree.SeekLE(buf)
		k, _ := biter.Deref()
		assert.True(t, bytes.Compare(k, buf) <= 0, "buf %d key %d", buf, k)
	}
}

func TestSeek(t *testing.T) {
	c := newC()
	maxkey := uint32(0)
	for i := 0; i <= 1211; i++ {
		a := RandRange(uint32(20), uint32(1000))
		maxkey = max(maxkey, a)
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, a)
		val := randomString(2000) // make a very long string
		c.tree.Insert(buf, []byte(val))
		c.ref[fmt.Sprint(i)] = val // reference data
	}
	for i := 0; i <= 11111; i++ {
		a := RandRange(uint32(20), uint32(1000))
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, a)
		biter := c.tree.Seek(buf, -3)
		k, _ := biter.Deref()
		assert.True(t, bytes.Compare(k, buf) <= 0, "buf %d key %d", buf, k)
	}
	for i := 0; i <= 11111; i++ {
		a := RandRange(uint32(20), uint32(1000))
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, a)
		biter := c.tree.Seek(buf, -2)
		k, _ := biter.Deref()
		assert.True(t, bytes.Compare(k, buf) < 0, "buf %d key %d", buf, k)
	}
	for i := 0; i <= 11111; i++ {
		a := RandRange(uint32(20), uint32(maxkey-1))
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, a)
		biter := c.tree.Seek(buf, 2)
		k, _ := biter.Deref()
		assert.True(t, bytes.Compare(k, buf) > 0, "buf %d key %d", buf, k)
	}
	for i := 0; i <= 11111; i++ {
		a := RandRange(uint32(20), uint32(maxkey-1))
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, a)
		biter := c.tree.Seek(buf, 3)
		k, _ := biter.Deref()
		assert.True(t, bytes.Compare(k, buf) >= 0, "buf %d key %d", buf, k)
	}
}
