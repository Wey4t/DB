package db

import (
	"encoding/binary"
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
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 2)
	assert.Equal(t, key[:4], buf)
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
	db.TableNew(&newtable)
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

	test_table := TableDef{
		Name:   "abc",
		Types:  []uint32{TYPE_BYTES, TYPE_INT64, TYPE_INT64, TYPE_INT64, TYPE_BYTES},
		Cols:   []string{"name", "age", "height", "weight", "address"},
		PKeys:  1,
		Prefix: 0,
	}

	db.TableNew(&test_table)
	// Sample Record 1: Alice
	rec1 := (&Record{}).
		AddStr("name", []byte("Bob")).
		AddInt64("age", 30).
		AddInt64("height", 165). // cm
		AddInt64("weight", 55).  // kg
		AddStr("address", []byte("123 Main St, New York"))
	db.Insert("abc", *rec1)
	search_rec = (&Record{}).AddStr("name", []byte("Bob"))
	assert.False(t, ValuesEqual(rec1.Vals, search_rec.Vals))
	ok, _ = db.Get("abc", search_rec)
	assert.True(t, ValuesEqual(rec1.Vals, search_rec.Vals))
	// Sample Record 2: Bob
	rec2 := (&Record{}).
		AddStr("name", []byte("Zob")).
		AddInt64("age", 25).
		AddInt64("height", 180).
		AddInt64("weight", 75).
		AddStr("address", []byte("456 Oak Ave, California"))

	// Sample Record 3: Charlie
	rec3 := (&Record{}).
		AddStr("name", []byte("Charlie")).
		AddInt64("age", 35).
		AddInt64("height", 175).
		AddInt64("weight", 70).
		AddStr("address", []byte("789 Pine Rd, Texas"))

	// Sample Record 4: Diana
	rec4 := (&Record{}).
		AddStr("name", []byte("Diana")).
		AddInt64("age", 28).
		AddInt64("height", 160).
		AddInt64("weight", 50).
		AddStr("address", []byte("321 Elm St, Florida"))

	// Sample Record 5: Eve
	rec5 := (&Record{}).
		AddStr("name", []byte("Eve")).
		AddInt64("age", 42).
		AddInt64("height", 170).
		AddInt64("weight", 65).
		AddStr("address", []byte("654 Maple Dr, Oregon"))

	// Sample Record 6: Frank
	rec6 := (&Record{}).
		AddStr("name", []byte("Frank")).
		AddInt64("age", 19).
		AddInt64("height", 185).
		AddInt64("weight", 80).
		AddStr("address", []byte("987 Cedar Ln, Washington"))

	// Test with various ages and sizes
	rec7 := (&Record{}).
		AddStr("name", []byte("Michael Jordan")).
		AddInt64("age", 60).
		AddInt64("height", 198).
		AddInt64("weight", 98).
		AddStr("address", []byte("Chicago Bulls Arena, Illinois"))

	rec8 := (&Record{}).
		AddStr("name", []byte("Serena Williams")).
		AddInt64("age", 42).
		AddInt64("height", 175).
		AddInt64("weight", 70).
		AddStr("address", []byte("Tennis Court Dr, Florida"))

	rec9 := (&Record{}).
		AddStr("name", []byte("Usain Bolt")).
		AddInt64("age", 37).
		AddInt64("height", 195).
		AddInt64("weight", 94).
		AddStr("address", []byte("Sprint Lane, Jamaica"))

	// Tech Workers
	rec10 := (&Record{}).
		AddStr("name", []byte("Sarah Chen")).
		AddInt64("age", 29).
		AddInt64("height", 162).
		AddInt64("weight", 52).
		AddStr("address", []byte("Silicon Valley Blvd, California"))

	rec11 := (&Record{}).
		AddStr("name", []byte("Alex Rodriguez")).
		AddInt64("age", 31).
		AddInt64("height", 177).
		AddInt64("weight", 73).
		AddStr("address", []byte("Tech Hub, Austin Texas"))

	rec12 := (&Record{}).
		AddStr("name", []byte("Priya Patel")).
		AddInt64("age", 26).
		AddInt64("height", 158).
		AddInt64("weight", 48).
		AddStr("address", []byte("Innovation St, Seattle"))

	// International Names
	rec13 := (&Record{}).
		AddStr("name", []byte("Hiroshi Tanaka")).
		AddInt64("age", 45).
		AddInt64("height", 170).
		AddInt64("weight", 65).
		AddStr("address", []byte("Tokyo Tower District, Japan"))
	rec14 := (&Record{}).
		AddStr("name", []byte("Emma Müller")).
		AddInt64("age", 33).
		AddInt64("height", 168).
		AddInt64("weight", 58).
		AddStr("address", []byte("Hauptstraße 42, Berlin Germany"))

	rec15 := (&Record{}).
		AddStr("name", []byte("Mohammed Al-Rashid")).
		AddInt64("age", 38).
		AddInt64("height", 182).
		AddInt64("weight", 78).
		AddStr("address", []byte("Palm Jumeirah, Dubai UAE"))

	// Students
	rec16 := (&Record{}).
		AddStr("name", []byte("Jessica Park")).
		AddInt64("age", 20).
		AddInt64("height", 165).
		AddInt64("weight", 55).
		AddStr("address", []byte("University Ave, Boston"))

	rec17 := (&Record{}).
		AddStr("name", []byte("Tyler Johnson")).
		AddInt64("age", 22).
		AddInt64("height", 183).
		AddInt64("weight", 76).
		AddStr("address", []byte("College Dorm, UCLA"))

	rec18 := (&Record{}).
		AddStr("name", []byte("Zoe Martinez")).
		AddInt64("age", 19).
		AddInt64("height", 160).
		AddInt64("weight", 50).
		AddStr("address", []byte("Campus Housing, Stanford"))

	// Senior Citizens
	rec19 := (&Record{}).
		AddStr("name", []byte("Robert Smith")).
		AddInt64("age", 72).
		AddInt64("height", 175).
		AddInt64("weight", 80).
		AddStr("address", []byte("Retirement Village, Florida"))

	rec20 := (&Record{}).
		AddStr("name", []byte("Margaret Thompson")).
		AddInt64("age", 68).
		AddInt64("height", 155).
		AddInt64("weight", 62).
		AddStr("address", []byte("Sunny Acres, Arizona"))

	// Healthcare Workers
	rec21 := (&Record{}).
		AddStr("name", []byte("Dr. Lisa Wong")).
		AddInt64("age", 41).
		AddInt64("height", 163).
		AddInt64("weight", 54).
		AddStr("address", []byte("Medical Center, San Francisco"))

	rec22 := (&Record{}).
		AddStr("name", []byte("Nurse James Brown")).
		AddInt64("age", 34).
		AddInt64("height", 179).
		AddInt64("weight", 72).
		AddStr("address", []byte("General Hospital, Chicago"))

	// Artists and Creative
	rec23 := (&Record{}).
		AddStr("name", []byte("Luna García")).
		AddInt64("age", 27).
		AddInt64("height", 167).
		AddInt64("weight", 59).
		AddStr("address", []byte("Art District, Los Angeles"))

	rec24 := (&Record{}).
		AddStr("name", []byte("Vincent O'Connor")).
		AddInt64("age", 39).
		AddInt64("height", 174).
		AddInt64("weight", 69).
		AddStr("address", []byte("Music Row, Nashville"))

	// Unusual Names
	rec25 := (&Record{}).
		AddStr("name", []byte("X Æ A-XII")).
		AddInt64("age", 4).
		AddInt64("height", 95).
		AddInt64("weight", 18).
		AddStr("address", []byte("Mars Colony Prep, California"))

	rec26 := (&Record{}).
		AddStr("name", []byte("Apple Gwyneth")).
		AddInt64("age", 15).
		AddInt64("height", 155).
		AddInt64("weight", 45).
		AddStr("address", []byte("Celebrity Hills, Hollywood"))

	// Business People
	rec27 := (&Record{}).
		AddStr("name", []byte("William Gates IV")).
		AddInt64("age", 52).
		AddInt64("height", 178).
		AddInt64("weight", 75).
		AddStr("address", []byte("Corporate Plaza, Redmond"))

	rec28 := (&Record{}).
		AddStr("name", []byte("Elizabeth Holmes")).
		AddInt64("age", 40).
		AddInt64("height", 169).
		AddInt64("weight", 61).
		AddStr("address", []byte("Innovation Campus, Palo Alto"))

	// Edge Cases - Very tall/short/heavy/light
	rec29 := (&Record{}).
		AddStr("name", []byte("Giant Pete")).
		AddInt64("age", 28).
		AddInt64("height", 220). // Very tall
		AddInt64("weight", 150). // Very heavy
		AddStr("address", []byte("Basketball Arena, NBA"))

	rec30 := (&Record{}).
		AddStr("name", []byte("Tiny Tim")).
		AddInt64("age", 25).
		AddInt64("height", 140). // Very short
		AddInt64("weight", 35).  // Very light
		AddStr("address", []byte("Circus Town, Nevada"))

	// Common Names (test duplicates if name isn't unique)
	rec31 := (&Record{}).
		AddStr("name", []byte("John Smith")).
		AddInt64("age", 35).
		AddInt64("height", 175).
		AddInt64("weight", 70).
		AddStr("address", []byte("Main Street, Anytown USA"))

	rec32 := (&Record{}).
		AddStr("name", []byte("Mary Johnson")).
		AddInt64("age", 28).
		AddInt64("height", 165).
		AddInt64("weight", 58).
		AddStr("address", []byte("Oak Avenue, Springfield"))

	rec33 := (&Record{}).
		AddStr("name", []byte("David Wilson")).
		AddInt64("age", 42).
		AddInt64("height", 180).
		AddInt64("weight", 82).
		AddStr("address", []byte("Elm Street, Georgetown"))

	// Historical Figures (for fun)
	rec34 := (&Record{}).
		AddStr("name", []byte("Albert Einstein")).
		AddInt64("age", 76).
		AddInt64("height", 175).
		AddInt64("weight", 70).
		AddStr("address", []byte("Princeton University, New Jersey"))

	rec35 := (&Record{}).
		AddStr("name", []byte("Marie Curie")).
		AddInt64("age", 66).
		AddInt64("height", 159).
		AddInt64("weight", 52).
		AddStr("address", []byte("Sorbonne University, Paris"))

	// All records array
	records := []*Record{
		rec2, rec3, rec4, rec5, rec6, rec7, rec8, rec9, rec10,
		rec11, rec12, rec13, rec14, rec15, rec16, rec17, rec18, rec19, rec20,
		rec21, rec22, rec23, rec24, rec25, rec26, rec27, rec28, rec29, rec30,
		rec31, rec32, rec33, rec34, rec35,
	}
	for _, rec := range records {
		db.Insert("abc", *rec)
	}
	search_rec = (&Record{}).AddStr("name", []byte("Marie Curie"))
	assert.False(t, ValuesEqual(rec1.Vals, search_rec.Vals))
	ok, _ = db.Get("abc", search_rec)
	assert.True(t, ValuesEqual(rec35.Vals, search_rec.Vals))
	delete_record := records[:len(records)-1]
	for _, rec := range delete_record {
		db.Delete("abc", *rec)
	}
	search_rec = (&Record{}).AddStr("name", []byte("Marie Curie"))
	assert.False(t, ValuesEqual(rec1.Vals, search_rec.Vals))
	ok, _ = db.Get("abc", search_rec)
	assert.True(t, ValuesEqual(rec35.Vals, search_rec.Vals))
}

func TestDel(t *testing.T) {

}
