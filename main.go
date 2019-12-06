package main

import (
	"log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	println("hello")
	///read
	fr, err := local.NewLocalFileReader("without_predefined_schema.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return
	}

	log.Println(res)

	pr.ReadStop()
	fr.Close()
}
