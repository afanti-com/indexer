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



type Indexer struct {
	idx_file_ string
	word_seq_ *int32
	pre_word_seq_ *int32
	partition_size_ uint
	seg *segment.Segment
	ht *hashtable.HashTable
	doc_info_ []utils.DocInfo
	index_buffer_ []bytes.Buffer
	pre_doc_seq_c_ []int
	index_dir_ string
	word_info_file_ string
	doc_info_file_ string
	logger *log.Logger
}


func (index *Indexer) Init(
	index_dir string,
	idx_file string,
	word_info_file string,
	doc_info_file string) {
	
	index.seg = new(segment.Segment)
	index.seg.Init()
	index.ht = new(hashtable.HashTable)
	index.ht.Init(1024*1024)
	index.doc_info_ = make([]utils.DocInfo, 100000)
	index.index_buffer_ = make([]bytes.Buffer, 300000)
	index.pre_doc_seq_c_ = make([]int, 300000)
	index.index_dir_ = index_dir
	index.idx_file_ = idx_file
	index.word_info_file_ = word_info_file
	index.doc_info_file_ = doc_info_file
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


func (index *Indexer) IndexAll(file string) {

	fin, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer fin.Close()

	var doc_seq int = 0	
	buf := bufio.NewReader(fin)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		line = strings.TrimSpace(line)
		items := strings.Split(line, "\t")

		question_id, _ := strconv.ParseInt(items[0], 10, 32)
		subject, _ := strconv.ParseInt(items[2], 10, 32)
		question := items[3]

		index.doc_info_[doc_seq].Id = int(question_id)
		index.doc_info_[doc_seq].DocLen = len(question)

		index.IndexOne(int(subject), question, doc_seq)

		doc_seq++
		index.logger.Printf("finish index doc: %d\n", doc_seq)
	}

	index.Flush()
	index.Merge()
	index.Complete(doc_seq)
}

func (index *Indexer) IndexOne(subject int, question string, doc_seq int) {

	is_math := false
	if subject == 2 || subject == 22 || subject == 42 {
		is_math = true
	}

	seg_result := index.seg.DoSegment(question)

	for _, sr := range seg_result {

		// index.logger.Printf("%s\t%s\t%d\n", sr.Word, sr.Tag, sr.Times)

		if !is_math && sr.Tag == "x" {
			continue
		}

		if is_math && !utils.IsAlpha(sr.Word) {
			continue
		}
		
		var word_id int32 = -1
		index.IndexWord(sr.Word, sr.Times, doc_seq, &word_id)
	}
}


func (index *Indexer) IndexWord(word string, times int, doc_seq int, word_id *int32) {
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
	defer fout.Close()
	
	_, err = index.index_buffer_[word_id].WriteTo(fout)
	if err != nil {
		panic(err)
	}
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
				fout.Close()
				panic(err)
			}
			fout.Close()
		}
	}
}

func (index *Indexer) Merge() {
	
	fout, err := os.OpenFile(index.idx_file_, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer fout.Close()

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
				panic("read write error")
			}

			if m < k_buf_size {
				break
			}
			
		}

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
		// index.logger.Println("offset: ", word_id, index_offset[word_id])
		fout.Write(buf.Bytes())
	}
}

func (index *Indexer) Complete(doc_seq int) {
	index.ht.Save(index.word_info_file_)
	index.SaveDocInfo(doc_seq)
	
}

func (index *Indexer) SaveDocInfo(doc_seq int) {
	fout, err := os.OpenFile(index.doc_info_file_, os.O_RDWR|os.O_CREATE, 0766)
	if err != nil {
		panic(err)
	}
	defer fout.Close()

	fout.WriteString(fmt.Sprintf("%d\n", doc_seq))

	for i := 0; i < doc_seq; i++ {
		fout.WriteString(fmt.Sprintf("%d %d\n", index.doc_info_[i].Id, index.doc_info_[i].DocLen))
	}
}
