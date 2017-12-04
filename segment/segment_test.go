package segment

import (
	"fmt"
	"testing"
)


func TestSearcher(t *testing.T) {

	var s string = "如图是动,植物细胞模式图,请据图回答 (1)植物细胞的最外层[ ]是 ,动物细胞的最外层[ ]是 . (2)图甲中的[C] ,能通过光合作用制造有机物和 . (3)图中的[D] 是遗传信息库. (4)西瓜甘甜可口,主要是因为[E] 内的细胞液中含有大量糖分. 如图是动,植物细胞模式图,请据图回答 (1)植物细胞的最外层[ ]是 ,动物细胞的最外层[ ]是 . (2)图甲中的[C] ,能通过光合作用制造有机物和 . (3)图中的[D] 是遗传信息库. (4)西瓜甘甜可口,主要是因为[E] 内的细胞液中含有大量糖分."

	seg := new(Segment)
	seg.Init()
	result := seg.DoSegment(s)

	for _, sr := range result {
		fmt.Printf("%s\t%s\t%d\n", sr.Word, sr.Tag, sr.Times)
	}
	
	seg.Free()
}
