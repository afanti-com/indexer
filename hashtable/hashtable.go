package hashtable

import (
	"io"
	"os"
	"fmt"
	"log"
	"bufio"
	"strings"
	"strconv"
	"../utils"
)


type HashEntry struct {
	key string
	word_id int32
	df uint32
	next *HashEntry
}


type HashTable struct {
	buckets_size_ uint32
	table_ []*HashEntry
	entry_num_ uint32
	used_buckets_ uint32
}


func (h *HashTable) Init(buckets_size uint32) {
	h.buckets_size_ = buckets_size
	h.entry_num_ = 0
	h.used_buckets_ = 0
	h.table_ = make([]*HashEntry, buckets_size)
}


func (h *HashTable) GetId(key string) int32 {
	index := h.HashFun(key)
	p := h.table_[index]
	
	for {
		if p == nil {
			return -1
		}

		if p.key == key {
			return p.word_id
		} else {
			p = p.next
		}
	}
}


func (h *HashTable) Insert(key string, word_seq *int32) int32 {
	index := h.HashFun(key)
	if (h.table_[index] == nil) {
		h.table_[index] = new(HashEntry)
		h.table_[index].key = key
		h.table_[index].word_id = *word_seq
		h.table_[index].df = 1
		h.table_[index].next = nil
		h.entry_num_++
		h.used_buckets_++
		(*word_seq)++
		return *word_seq - 1
	} else {
		p := h.table_[index]
		pre := p

		for {
			if p == nil{
				break
			}
			if p.key == key {
				break
			}
			pre = p
			p = p.next
		}
		
		if p == nil {
			pre.next = new(HashEntry)
			pre.next.key = key
			pre.next.word_id = *word_seq
			pre.next.df = 1
			pre.next.next = nil
			h.entry_num_++
			(*word_seq)++
			return *word_seq -1
		} else {
			p.df++
			return p.word_id
		}
	}
}


func (h HashTable) HashFun(key string) uint32 {
	var sum uint32 = 0
	for _, c := range key {
		sum = ((sum << 5) + sum + uint32(c));
	}
	return sum % h.buckets_size_
}


func (h HashTable) Show() {
	for i, item := range h.table_ {
		for {
			if item == nil {
				break
			}
			
			idf := utils.ScoreIdf(10000000, item.df)
			buf := fmt.Sprintf("%d %s %d %d %f\n", i, item.key, item.word_id, item.df, idf)
			fmt.Printf(buf)
			item = item.next
		}
	}
}


func (h HashTable) Save(file string) error {
	fout, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0766)
	if err != nil {
		log.Fatal(err)
		return err
	}
	
	defer fout.Close()
	
	for i, item := range h.table_ {
		for {
			if item == nil {
				break
			}
			
			idf := utils.ScoreIdf(10000000, item.df)
			fout.WriteString(fmt.Sprintf("%d %s %d %d %f\n",
				i, item.key, item.word_id, item.df, idf))
			item = item.next
		}
	}
	
	return nil
}


func (h HashTable) Load(file string) error {
	fin, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer fin.Close()
	
	buf := bufio.NewReader(fin)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		
		line = strings.TrimSpace(line)
		
		items := strings.Split(line, " ")
		if len(items) != 5 {
			break
		}

		index, _ := strconv.ParseInt(items[0], 10, 32)
		key := items[1]
		raw_word_id, _ := strconv.ParseInt(items[2], 10, 32)
		word_id := int32(raw_word_id)
		raw_df, _ := strconv.ParseInt(items[3], 10, 32)
		df := uint32(raw_df)

		// idf, e := strconv.ParseFloat(items[4], 64)

		var pre *HashEntry = nil
		
		// load into hash
		if h.table_[index] == nil {
			h.table_[index] = new(HashEntry)
			h.table_[index].key = key
			h.table_[index].word_id = word_id
			h.table_[index].df = df
			h.table_[index].next = nil
			pre = h.table_[index]
			h.entry_num_++
			h.buckets_size_++
		} else {
			p := new(HashEntry)
			p.key = key
			p.word_id = word_id
			p.df = df
			p.next = nil
			pre.next = p
			pre = p
			h.entry_num_++
		}
	}

	return nil
}
