package indexer

import (
	"fmt"
	"os"
	"io"
	"log"
	"bufio"
	"bytes"
	"strings"
	"strconv"
	"unsafe"
	"encoding/binary"
	"../segment"
	"../hashtable"
	"../compressor"
	"../utils"
)


type DocInfo struct {
	id int
	doc_len int
	wordlist_len int
}

type Indexer struct {
	idx_file_ string
	word_seq_ *int32
	pre_word_seq_ *int32
	partition_size_ uint
	seg *segment.Segment
	ht *hashtable.HashTable
	doc_info_ []DocInfo
	index_buffer_ []bytes.Buffer
	pre_doc_seq_c_ []int
	index_dir_ string
	logger *log
}


func (index *Indexer) Init(index_dir string, idx_file string){
	index.seg = new(segment.Segment)
	index.seg.Init()
	index.ht = new(hashtable.HashTable)
	index.ht.Init(1024*1024)
	index.doc_info_ = make([]DocInfo, 100000)
	index.index_buffer_ = make([]bytes.Buffer, 300000)
	index.pre_doc_seq_c_ = make([]int, 300000)
	index.index_dir_ = index_dir
	index.idx_file_ = idx_file
	index.word_seq_ = new(int32)
	index.pre_word_seq_ = new(int32)
	*(index.word_seq_) = 0
	*(index.pre_word_seq_) = 0
	index.partition_size_ = 128
	index.logger = log.New(os.Stdout, "[indexer] ", log.Ldate|log.Ltime|log.Lshortfile)
}

func (index *Indexer) Release(){
	index.seg.Free()
}


func (index *Indexer) IndexAll(file string) int {

	fin, err := os.Open(file)
	if err != nil {
		index.logger.Fatal(err)
		return -1
	}


	var doc_seq int = 0	
	buf := bufio.NewReader(fin)
	for {

		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return -1
		}

		line = strings.TrimSpace(line)
		items := strings.Split(line, "\t")

		question_id, _ := strconv.ParseInt(items[0], 10, 32)
		subject, _ := strconv.ParseInt(items[2], 10, 32)
		question := items[3]

		index.doc_info_[doc_seq].id = int(question_id)
		index.doc_info_[doc_seq].doc_len = len(question)

		index.IndexOne(int(subject), question, doc_seq)

		doc_seq++
		index.logger.Printf("finish doc: %d\n", doc_seq)
		
	}
	
	fin.Close()

	index.Flush()

	index.Merge()

	/*
	index.Complete()
  */
	
	return 0
}

func (index *Indexer) IndexOne(subject int,
	question string,
	doc_seq int) int {

	is_math := false
	if subject == 2 || subject == 22 || subject == 42 {
		is_math = true
	}

	result := make([]segment.SegRes, 0)
	index.seg.DoSegment(question, &result)

	for _, sr := range result {
		// index.logger.Printf("%s\t%s\t%d\n", sr.Word, sr.Tag, sr.Times)

		if !is_math && sr.Tag == "x" {
			continue
		}

		if is_math && !utils.IsAlpha(sr.Word) {
			continue
		}
		
		var word_id int32 = -1
		index.IndexWord(
			sr.Word,
			sr.Times,
			doc_seq,
			&word_id)
	}

	return 0
}


func (index *Indexer) IndexWord(
	word string,
	times int,
	doc_seq int,
	word_id *int32) int{

	*word_id = index.ht.Insert(word, index.word_seq_)
	if *(index.word_seq_) > *(index.pre_word_seq_) {
		*(index.pre_word_seq_) = *(index.word_seq_)
	}
	
	inter := doc_seq - index.pre_doc_seq_c_[*word_id];
	index.pre_doc_seq_c_[*word_id] = doc_seq

	compressor.Encode(&index.index_buffer_[*word_id], uint32(inter))
	compressor.Encode(&index.index_buffer_[*word_id], uint32(times))

	if len((index.index_buffer_[*word_id]).Bytes()) > 4096 {
		index.WriteBuffer(*word_id)
	}

	return 0
}



func (index *Indexer) WriteBuffer(word_id int32) {

	filename := fmt.Sprintf("%s/%d/%d",
		index.index_dir_,
		uint(word_id) % index.partition_size_,
		word_id)
	
	fout, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	_, err = index.index_buffer_[word_id].WriteTo(fout)
	if err != nil {
		panic(err)
	}
	
	fout.Close()	
}


func (index *Indexer) Flush() {
	var word_id int32 = 0
	for ; word_id < *index.word_seq_; word_id++ {
		if len(index.index_buffer_[word_id].Bytes()) > 0 {
			filename := fmt.Sprintf("%s/%d/%d",
				index.index_dir_,
				uint(word_id) % index.partition_size_,
				word_id)
			fout, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
			
			_, err = index.index_buffer_[word_id].WriteTo(fout)
			if err != nil {
				panic(err)
			}
			
			fout.Close()	
		}
	}
}

func (index *Indexer) Merge() int {
	
	fout, err := os.OpenFile(index.idx_file_, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	index_offset := make([]int64, *index.word_seq_)
	var offset int64 = 0
	var first_pos int64 = int64((*index.word_seq_) + 1) * int64(unsafe.Sizeof(offset))
	offset, err = fout.Seek(first_pos, 0)
	if err != nil {
		panic(err)
	}
	
	var word_id int32 = 0
	for ; word_id < *index.word_seq_; word_id++ {
		filename := fmt.Sprintf("%s/%d/%d",
			index.index_dir_,
			uint(word_id) % index.partition_size_,
			word_id)
		fin, err := os.Open(filename)
		if err != nil {
			index_offset[word_id] = offset
			continue
		}

		var k_buf_size int = 4096 * 1024

		buf := make([]byte, k_buf_size)
		for {
			m, err := fin.Read(buf)
			if err != nil {
				panic(err)
			}	
			n, err := fout.Write(buf[:m]) 
			if err != nil {
				panic(err)
			}

			if m != n {
				index.logger.Fatal("read write error")
				return -1
			}

			if m < k_buf_size {
				break
			}
			
		}
		fin.Close()

		offset, err = fout.Seek(0, os.SEEK_CUR)
		index_offset[word_id] = offset
	}
		
	offset, err = fout.Seek(0, 0)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, first_pos)
	fout.Write(buf.Bytes())

	word_id = 0
	for ; word_id < *index.word_seq_; word_id++ {
		buf = new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, index_offset[word_id])
		index.logger.Println("offset: ", word_id, index_offset[word_id])
		fout.Write(buf.Bytes())
	}
	
	fout.Close()
	return 0
}
