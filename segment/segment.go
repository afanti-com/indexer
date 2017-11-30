package segment

import (
	"strings"
	"github.com/yanyiwu/gojieba"
)


type SegRes struct {
	Word string
	Tag string
	Times int
	Offset int
}


type Segment struct {
	jieba *gojieba.Jieba
}


func (s *Segment) Init(){
	s.jieba = gojieba.NewJieba()
}

func (s *Segment) Free() {
	s.jieba.Free()
}


func (s *Segment) DoSegment(text string, result *[]SegRes) {
	// TODO: For simplicity, use gojieba module first
	wordsMap := make(map[string]SegRes)
	words := s.jieba.Cut(text, true)
	for _, word := range words {
		tag := strings.Split(strings.Join(s.jieba.Tag(word), ""), "/")[1]
		_, ok := wordsMap[word]
		if !ok {
			wordsMap[word] = SegRes{word, tag, 1, 0}
		} else {
			sr := wordsMap[word]
			sr.Times += 1
			wordsMap[word] = sr
		}
	}

	for _, sr := range wordsMap {
		*result = append(*result, sr)
	}
	
}
