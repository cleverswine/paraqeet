package main

import (
	"encoding/json"
	"io"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// ParquetToJSON writes the parquet file to json
func ParquetToJSON(fn string, w io.Writer, limit int) error {
	fr, err := local.NewLocalFileReader(fn)
	if err != nil {
		return err
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return err
	}
	defer pr.ReadStop()
	num := limit
	if num < 1 {
		num = int(pr.GetNumRows())
	}
	res, err := pr.ReadByNumber(num)
	if err != nil {
		return err
	}
	m := json.NewEncoder(w)
	return m.Encode(res)
}
