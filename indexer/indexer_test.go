package indexer

import "testing"

func TestSearcher(t *testing.T) {
	indexer := new(Indexer)
	indexer.Init(
		"../data/index_data_tmp/",
		"../data/index.dat",
		"../data/word_info_file.dat",
		"../data/doc_info_file.dat")

	// TODO
	
	indexer.Release()
}

