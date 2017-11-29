package main

import (
	"fmt"
	"os"
	"./indexer"
)


func main() {
	fmt.Println("index begin....")
	
	indexer := new(indexer.Indexer)
	indexer.Init("./data/index_data_tmp/", "./data/index.dat")

	filename :=  os.Args[1]
	indexer.IndexAll(filename)

	indexer.Release()

	fmt.Println("index done")
}

