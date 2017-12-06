package hashtable

import (
	"testing"
)


func TestHashtableInsertAndGet(t *testing.T) {
	var size uint32 = 128
	var word_id int32 = 0
	var word_seq int32 = 0
	
	ht := new(HashTable)
	ht.Init(size)

	word_id = ht.Insert("hello", &word_seq)

	if word_id != 0 || word_seq != 1 {
		t.Errorf("hashtable insert uniq key error")
	}
	word_id = ht.Insert("world", &word_seq)
	if word_id != 1 || word_seq != 2 {
		t.Errorf("hashtable insert uniq key error")
	}
	
	word_id = ht.Insert("hello", &word_seq)
	if word_id != 0 || word_seq != 2 {
		t.Errorf("hashtable insert repetitive key error")
	}

	word_id = ht.GetId("world")
	if word_id != 1 {
		t.Errorf("hashtable get key error")
	}
}

func TestHashtableShow(t *testing.T) {
	var size uint32 = 128
	var word_seq int32 = 0
	ht := new(HashTable)
	ht.Init(size)
	ht.Insert("hello", &word_seq)
	ht.Insert("world", &word_seq)
	ht.Insert("hello", &word_seq)

	ht.Show()
	ret := ht.Save("word_info.dat")
	if ret != nil {
		t.Errorf("hashtable save word info error")
	}
	
	new_ht := new(HashTable)
	new_ht.Init(size)
	ret = new_ht.Load("word_info.dat")
	if ret != nil {
		t.Errorf("hashtable load word info error")
	}
	
	new_ht.Show()
}

