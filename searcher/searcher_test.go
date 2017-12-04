package searcher

import (
	"fmt"
	"testing"
)


func TestSearcher(t *testing.T) {
	s := new(Searcher)
	s.Init("../data/index.dat", "../data/word_info_file.dat", "../data/doc_info_file.dat")
	result := s.Search("如图是动,植物细胞模式图,请据图回答 (1)植物细胞的最外层[ ]是 ,动物细胞的最外层[ ]是 . (2)图甲中的[C] ,能通过光合作用制造有机物和 . (3)图中的[D] 是遗传信息库. (4)西瓜甘甜可口,主要是因为[E] 内的细胞液中含有大量糖分. 如图是动,植物细胞模式图,请据图回答 (1)植物细胞的最外层[ ]是 ,动物细胞的最外层[ ]是 . (2)图甲中的[C] ,能通过光合作用制造有机物和 . (3)图中的[D] 是遗传信息库. (4)西瓜甘甜可口,主要是因为[E] 内的细胞液中含有大量糖分.")

	for i, value := range result {
		fmt.Println(i, value.Qid, value.Score)
	}
	
	if result[0].Qid != 21016186 {
		t.Errorf("search result error")		
	}

	s.Release()
}
