package db

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	. "server"
	. "utils"
)

const TABLE_PREFIX_MIN = 1
const (
	TYPE_ERROR uint32 = iota
	TYPE_BYTES
	TYPE_INT64
)

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(key string, val []byte) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})
	return rec
}
func (rec *Record) AddInt64(key string, val int64) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})
	return rec
}
func (rec *Record) Get(key string) *Value {
	for i, col := range rec.Cols {
		if col == key {
			return &rec.Vals[i]
		}
	}
	return &Value{Type: TYPE_ERROR} // not found
}

type DB struct {
	Path string
	// internals
	kv     KV
	tables map[string]*TableDef // cached table definition
}

// table definition
type TableDef struct {
	// user defined
	Name  string
	Types []uint32 // column types
	Cols  []string // column names
	PKeys int      // the first PKeys columns are the primary key
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
}

// internal table: metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

// get a single row by the primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	// fmt.Println("prefix", tdef.Prefix, "values:", values[:tdef.PKeys], "record", rec)
	// fmt.Printf("serach for key: %s\n", key)
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}
	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.PKeys:])
	rec.Cols = append(rec.Cols, tdef.Cols[tdef.PKeys:]...)
	rec.Vals = append(rec.Vals, values[tdef.PKeys:]...)
	return true, nil
}

// reorder a record and check for missing columns.
// n == tdef.PKeys: record is exactly a primary key
// n == len(tdef.Cols): record contains all columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	// omitted...
	if n < tdef.PKeys || n > len(tdef.Cols) {
		return nil, errors.New("invalid record length")
	}

	if n == tdef.PKeys {
		// primary key only
		values := make([]Value, len(tdef.Cols))
		for i := 0; i < tdef.PKeys; i++ {
			values[i] = *rec.Get(tdef.Cols[i])
			if values[i].Type != tdef.Types[i] {
				return nil, errors.New("invalid type for primary key")
			}
		}
		return values, nil
	}
	if n == len(tdef.Cols) {
		values := make([]Value, len(tdef.Cols))
		for i := 0; i < n; i++ {
			values[i] = *rec.Get(tdef.Cols[i])
			if values[i].Type != tdef.Types[i] {
				return nil, errors.New("invalid type for primary key")
			}
		}
		return values, nil
	}

	return nil, errors.New("record must contain primary key columns only")
}

func encodeValues(out []byte, vals []Value) []byte {
	for _, val := range vals {
		encodeVal, _ := json.Marshal(val)
		uint32Len := uint32(len(encodeVal))
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32Len)
		out = append(out, buf[:]...) // write the length of the value
		out = append(out, encodeVal...)
	}
	return out // omitted: encode each Value to the output slice
}
func decodeValues(in []byte, out []Value) {
	length := binary.BigEndian.Uint32(in[:4])
	Assert(len(out) > 0)
	count := 0
	for length > 0 && count < len(out) {
		val := Value{}
		json.Unmarshal(in[4:4+length], &val)
		out[count] = val
		count += 1
		in = in[4+length:]
		length = binary.BigEndian.Uint32(in[:4])
	}
}

// for primary keys
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

// get the table definition by name
func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}
func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	// fmt.Println("get the table def from intenal")
	ok, err := dbGet(db, TDEF_TABLE, rec)
	Assert(err == nil)
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	Assert(err == nil)
	return tdef
}
