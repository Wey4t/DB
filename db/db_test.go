package db

import (
	"encoding/json"
	"fmt"
	"os"
	. "server"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecord(t *testing.T) {
	rec := &Record{}
	rec.AddStr("name", []byte("Alice"))
	rec.AddInt64("age", 30)
	assert.Equal(t, []byte("Alice"), rec.Get("name").Str, "Expected name to be Alice")
	assert.Equal(t, int64(30), rec.Get("age").I64, "Expected name to be Alice")
}

func TestEncodeKey(t *testing.T) {
	val := Value{
		Type: TYPE_BYTES,
		Str:  []byte("Alice"),
		I64:  0,
	}
	vals := []Value{val}
	key := encodeKey(nil, 2, vals)
	fmt.Println("Encoded key:", key)

}

func ValuesEqual(a, b []Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].I64 != b[i].I64 || string(a[i].Str) != string(b[i].Str) {
			return false
		}
	}
	return true
}
func TestEncodeValue(t *testing.T) {
	val1 := Value{
		Type: TYPE_BYTES,
		Str:  []byte("Alice"),
		I64:  0,
	}
	val2 := Value{
		Type: TYPE_INT64,
		Str:  nil,
		I64:  333,
	}
	val3 := Value{
		Type: TYPE_BYTES,
		Str:  []byte("Bob"),
		I64:  0,
	}
	vals := []Value{val1, val2, val2, val2, val2, val2, val3}
	encoded := encodeValues(nil, vals)
	decoded := make([]Value, len(vals))
	decodeValues(encoded, decoded[:])
	assert.True(t, ValuesEqual(vals, decoded), "Encoded and decoded values should be equal")
}
func TestCheckRecord(t *testing.T) {
	tdef := &TableDef{
		Prefix: 2,
		Name:   "test_table",
		Types:  []uint32{TYPE_BYTES, TYPE_INT64},
		Cols:   []string{"name", "age"},
		PKeys:  1,
	}

	rec := Record{}
	rec.AddStr("name", []byte("Alice"))
	rec.AddInt64("age", 30)

	values, err := checkRecord(tdef, rec, tdef.PKeys)
	assert.NoError(t, err, "Expected no error for valid record")
	assert.Equal(t, 2, len(values), "Expected two values in the record")
	assert.Equal(t, TYPE_BYTES, values[0].Type, "Expected first value to be TYPE_BYTES")
	assert.Equal(t, []byte("Alice"), values[0].Str, "Expected name to be Alice")
	assert.Equal(t, TYPE_ERROR, values[1].Type, "Expected second value to be TYPE_INT64")
	assert.Equal(t, int64(0), values[1].I64, "Expected age to be 30")
}

func TestNewTable(t *testing.T) {
	kv := NewKv("test_page.txt")
	kv.Open()
	defer os.Remove("test_page.txt")
	defer kv.Close()
	db := &DB{
		kv:     *kv,
		tables: map[string]*TableDef{},
		Path:   "test_page.txt",
	}

	newtable := TableDef{
		Name:   "test_table",
		Types:  []uint32{TYPE_BYTES, TYPE_INT64},
		Cols:   []string{"name", "age"},
		PKeys:  1,
		Prefix: 0,
	}
	def, err := json.Marshal(newtable)
	if err != nil {
		panic("a")
	}
	def_record := (&Record{}).AddStr("name", []byte("test_table")).AddStr("def", def)
	dbUpdate(db, TDEF_TABLE, *def_record, 0)
	rec := (&Record{}).AddStr("name", []byte("Alice")).AddInt64("age", 30)
	search_rec := (&Record{}).AddStr("name", []byte("Alice"))

	db.Insert("test_table", *rec)

	ok, _ := db.Get("test_table", search_rec)
	assert.True(t, ok)
	assert.Equal(t, int64(30), search_rec.Vals[1].I64)
	// test update
	update_rec := (&Record{}).AddStr("name", []byte("Alice")).AddInt64("age", 333)
	// db.kv.Debug("b")s

	db.Update("test_table", *update_rec)
	// db.kv.Debug("a")
	search_rec = (&Record{}).AddStr("name", []byte("Alice"))
	ok, _ = db.Get("test_table", search_rec)
	assert.True(t, ok)
	assert.Equal(t, int64(333), search_rec.Vals[1].I64)
}
