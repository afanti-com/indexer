package searcher

import (
	_ "fmt"
	"os"
	"io"
	"bytes"
	"unsafe"
	"bufio"
	"strings"
	"strconv"
	"encoding/binary"
	"log"
	"sort"
	"../segment"
	"../compressor"
	"../utils"
)


type WordInfo struct {
	word_id int32
	df uint32
}

type SearchRes struct {
	Qid   uint32
	Score float64
}

type SearchResList []SearchRes

func (p SearchResList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p SearchResList) Len() int           { return len(p) }
func (p SearchResList) Less(i, j int) bool { return p[i].Score > p[j].Score }


type Searcher struct {
	idx_file_ string
	offset_list_ []int64
	doc_info_ []utils.DocInfo
	word_info_ map[string]WordInfo
	seg *segment.Segment
	logger *log.Logger
	total_doc_ uint32
}


func (s *Searcher) Init(idx_file string, word_info_file string, doc_info_file string) {
	s.logger = log.New(os.Stdout, "[searcher] ", log.Ldate|log.Ltime|log.Lshortfile)
	s.idx_file_ = idx_file
	s.seg = new(segment.Segment)
	s.seg.Init()
	s.word_info_ = make(map[string]WordInfo)
	s.LoadWordInfo(word_info_file)
	s.LoadDocInfo(doc_info_file)
	s.LoadOffset()
	
}

func (s *Searcher) Release(){
	s.seg.Free()
}

func (s *Searcher) SortMapByValue(m map[uint32]float64) SearchResList {
	p := make(SearchResList, len(m))
	i := 0
	for k, v := range m {
		qid := uint32(s.doc_info_[k].Id)
		p[i] = SearchRes{qid, v}
		i++
	}
	sort.Sort(p)
	return p
}


func (s *Searcher) LoadOffset() {
	fin, err := os.Open(s.idx_file_)
	if err != nil {
		panic(err)
	}

	var offset_size int64 = 0
	bb := make([]byte, unsafe.Sizeof(offset_size))
	_, err = fin.Read(bb)
	if err != nil {
		panic(err)
	}

	_ = binary.Read(bytes.NewReader(bb), binary.LittleEndian, &offset_size)
	offset_size /=  int64(unsafe.Sizeof(offset_size))
	s.offset_list_ = make([]int64, offset_size)

	var i int64 = 0
	for ; i < offset_size; i++ {
		var one int64 = 0
		bb := make([]byte, unsafe.Sizeof(offset_size))
		_, err = fin.Read(bb)
		if err != nil {
			panic(err)
		}
		_ = binary.Read(bytes.NewReader(bb), binary.LittleEndian, &one)
		s.offset_list_[i] = one
	}
	
	fin.Close()
}


func (s *Searcher) GetPosting(id int64, result *[]uint32){
	len := s.offset_list_[id + 1] - s.offset_list_[id]
	if len > 0 {
		fin, err := os.Open(s.idx_file_)
		if err != nil {
			panic(err)
		}
		_, err = fin.Seek(s.offset_list_[id], 0)

		bb := make([]byte, len)
		_, err = fin.Read(bb)
		if err != nil {
			panic(err)
		}
		
		var buf bytes.Buffer
		_, err = buf.Write(bb)
		compressor.Decode(&buf, uint(len), result)
		fin.Close()
	}
}


func (s *Searcher) Search(query string) SearchResList{

	seg_result := s.seg.DoSegment(query)

	is_math := false
	score_map := make(map[uint32]float64)
	
	for _, sr := range seg_result {

		// fmt.Printf("%s\t%s\t%d\n", sr.Word, sr.Tag, sr.Times)
		
		if !is_math && sr.Tag == "x" {
			continue
		}
		
		if is_math && !utils.IsAlpha(sr.Word) {
			continue
		}

		wordinfo, ok := s.word_info_[sr.Word]
		if !ok {
			continue
		}

		word_id := wordinfo.word_id
		df := wordinfo.df
		var pre_doc_id uint32 = 0
		var doc_id uint32 = 0
		var freq uint32 = 0
		// s.logger.Printf("word_id = %d", word_id)
		result := make([]uint32, 0)
		s.GetPosting(int64(word_id), &result)
		// fmt.Println(" ===> result:", result)
		for index, value := range result {
			if index % 2 == 0 {
				doc_id = value + pre_doc_id
				pre_doc_id = doc_id
			} else {
				freq = value
				// fmt.Println("doc_id:", doc_id, "freq:", freq)
				score, ok := score_map[doc_id]
				if !ok {
					score_map[doc_id] = float64(freq) * utils.ScoreIdf(s.total_doc_, df)
				} else {
					score_map[doc_id] = score + float64(freq) * utils.ScoreIdf(s.total_doc_, df)
				}
			}
		}
	}

	sort_result := s.SortMapByValue(score_map)
	return sort_result
}


func (s *Searcher) LoadDocInfo(doc_info_file string) int {

	fin, err := os.Open(doc_info_file)
	if err != nil {
		s.logger.Fatal(err)
		return -1
	}

	var doc_seq int64 = 0
	buf := bufio.NewReader(fin)
	line, err := buf.ReadString('\n')
	if err != nil {
		return -1
	}

	line = strings.TrimSpace(line)
	total_doc, _ := strconv.ParseInt(line, 10, 32)
	s.total_doc_ = uint32(total_doc)
	s.doc_info_ = make([]utils.DocInfo, s.total_doc_)
	
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fin.Close()
			return -1
		}

		line = strings.TrimSpace(line)
		items := strings.Split(line, " ")
		question_id, _ := strconv.ParseInt(items[0], 10, 32)
		doc_len, _ := strconv.ParseInt(items[1], 10, 32)
		s.doc_info_[doc_seq].Id = int(question_id)
		s.doc_info_[doc_seq].DocLen = int(doc_len)
		doc_seq++
	}

	fin.Close()

	if doc_seq != total_doc {
		s.logger.Printf("oc_seq != total_doc\n")
		return -1
	}

	return 0
}


func (s *Searcher) LoadWordInfo(word_info_file string) int {

	fin, err := os.Open(word_info_file)
	if err != nil {
		s.logger.Fatal(err)
		return -1
	}

	buf := bufio.NewReader(fin)

	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fin.Close()
			return -1
		}

		line = strings.TrimSpace(line)
		items := strings.Split(line, " ")

		if len(items) != 5 {
			s.logger.Printf("word info format error\n")
			return -1
		}

		// index, _ := strconv.ParseInt(items[0], 10, 32)
		strconv.ParseInt(items[0], 10, 32)
		key := items[1]
		raw_word_id, _ := strconv.ParseInt(items[2], 10, 32)
		word_id := int32(raw_word_id)
		raw_df, _ := strconv.ParseInt(items[3], 10, 32)
		df := uint32(raw_df)

		/*
		s.logger.Printf("index=%d, key=[%s], word_id=%d, df = %d\n",
			index, key, word_id, df)
    */
		
		s.word_info_[key] = WordInfo{word_id:word_id, df:df}
	}

	fin.Close()

	return 0
}
