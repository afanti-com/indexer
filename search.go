package main

import (
	"os"
	"log"
	"fmt"
	"./searcher"
)


func main() {
	
	logger := log.New(os.Stdout, "[searcher] ", log.Ldate|log.Ltime|log.Lshortfile)
	logger.Println("search begin....")

	s := new(searcher.Searcher)
	s.Init("./data/index.dat", "./data/word_info_file.dat", "./data/doc_info_file.dat")

	query :=  os.Args[1]
	result := s.Search(query)
	
	for _, value := range result {
		fmt.Println(value.Qid, value.Score)
	}
	
	s.Release()
}
