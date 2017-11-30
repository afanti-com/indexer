package main

import (
	"os"
	"log"
	"./indexer"
)


func main() {
	logger := log.New(os.Stdout, "[indexer] ", log.Ldate|log.Ltime|log.Lshortfile)
	logger.Println("index begin....")
	
	indexer := new(indexer.Indexer)
	indexer.Init("./data/index_data_tmp/", "./data/index.dat")

	// input filename 
	filename :=  os.Args[1]
	indexer.IndexAll(filename)

	indexer.Release()

	logger.Println("index done")
}

