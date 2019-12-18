package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type File struct {
	fileName    string
	columnNames []string
	data        []map[string]interface{}
}

func NewFileFromData(data []map[string]interface{}, sortBy []string) (*File, error) {
	f := &File{
		fileName: "",
	}
	for k := range data[0] {
		f.columnNames = append(f.columnNames, k)
	}
	f.data = data
	if sortBy == nil || len(sortBy) == 0 {
		return f, nil
	}
	sort.Slice(f.data, func(i, j int) bool {
		return getComposite(f.data[i], sortBy) < getComposite(f.data[j], sortBy)
	})
	return f, nil
}

func NewFile(fn string, limit int, sortBy []string) (*File, error) {
	f := &File{
		fileName: fn,
	}
	fr, err := local.NewLocalFileReader(f.fileName)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, nil, 2)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()
	// col names
	colNumber := len(pr.SchemaHandler.IndexMap)
	prefix := pr.SchemaHandler.IndexMap[0]
	f.columnNames = []string{}
	prev := ""
	for i := 1; i < colNumber; i++ {
		thisOne := strings.Split(pr.SchemaHandler.IndexMap[int32(i)], ".")[1]
		if thisOne != prev {
			f.columnNames = append(f.columnNames, strings.Replace(thisOne, prefix+".", "", -1))
			prev = thisOne
		}
	}
	// num rows, limit
	totalRows := int(pr.GetNumRows())
	l := limit
	if limit < 1 || limit >= totalRows {
		l = totalRows
	}
	// read data
	res, err := pr.ReadByNumber(l)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	data, err := toMap(res)
	if err != nil {
		return nil, err
	}
	f.data = data
	if sortBy == nil || len(sortBy) == 0 {
		return f, nil
	}
	sort.Slice(f.data, func(i, j int) bool {
		return getComposite(f.data[i], sortBy) < getComposite(f.data[j], sortBy)
	})
	return f, nil
}

func (f *File) ColumnNames() []string {
	return f.columnNames
}

func (f *File) Info() map[string]interface{} {
	return map[string]interface{}{
		"ColumnNames": f.columnNames,
		"TotalRows":   len(f.data),
	}
}

func (f *File) ToJSON(w io.Writer) error {
	j := json.NewEncoder(w)
	return j.Encode(f.data)
}

func (f *File) ToTable(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 0, '.', tabwriter.Debug)
	fmt.Fprintln(tw, strings.Join(f.columnNames, "\t"))
	for i := 0; i < len(f.data); i++ {
		res := []string{}
		row := f.data[i]
		for j := 0; j < len(f.columnNames); j++ {
			res = append(res, valToString(row[f.columnNames[j]]))
		}
		fmt.Fprintln(tw, strings.Join(res, "\t"))
	}
	return tw.Flush()
}

func (f *File) Data() []map[string]interface{} {
	return f.data
}

func getComposite(data map[string]interface{}, cols []string) string {
	result := ""
	for i := 0; i < len(cols); i++ {
		if v, ok := data[cols[i]]; ok {
			result = result + valToString(v)
		}
	}
	return result
}

func toMap(res []interface{}) ([]map[string]interface{}, error) {
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