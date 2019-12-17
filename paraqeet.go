package main

import (
	"encoding/json"
	"io"
	"sort"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type Paraqeet struct {
	ColumnNames []string
	TotalRows   int
	pr          *reader.ParquetReader
}

func NewParaqeet(fn string) (*Paraqeet, error) {
	fr, err := local.NewLocalFileReader(fn)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, err
	}
	// col names
	colNumber := len(pr.SchemaHandler.IndexMap)
	prefix := pr.SchemaHandler.IndexMap[0]
	columns := []string{}
	prev := ""
	for i := 1; i < colNumber; i++ {
		thisOne := strings.Split(pr.SchemaHandler.IndexMap[int32(i)], ".")[1]
		if thisOne != prev {
			columns = append(columns, strings.Replace(thisOne, prefix+".", "", -1))
			prev = thisOne
		}
	}
	return &Paraqeet{
		pr:          pr,
		ColumnNames: columns,
		TotalRows:   int(pr.GetNumRows()),
	}, nil
}

func (f *Paraqeet) Read(limit int, sortBy []string) ([]map[string]interface{}, error) {
	l := limit
	if limit < 1 || limit > f.TotalRows {
		l = f.TotalRows
	}
	res, err := f.pr.ReadByNumber(l)
	if err != nil {
		return nil, err
	}
	data, err := f.toMap(res)
	if err != nil {
		return nil, err
	}
	if sortBy == nil || len(sortBy) == 0 {
		return data, nil
	}
	sort.Slice(data, func(i, j int) bool {
		return getComposite(data[i], sortBy) > getComposite(data[j], sortBy)
	})
	return data, nil
}

func (f *Paraqeet) Info(w io.Writer) error {
	j := json.NewEncoder(w)
	return j.Encode(f)
}

func (f *Paraqeet) ToJson(limit int, w io.Writer) error {
	data, err := f.Read(limit, nil)
	if err != nil {
		return err
	}
	j := json.NewEncoder(w)
	return j.Encode(data)
}

func (f *Paraqeet) toMap(res []interface{}) ([]map[string]interface{}, error) {
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	m := []map[string]interface{}{}
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (f *Paraqeet) Close() {
	f.pr.ReadStop()
}
