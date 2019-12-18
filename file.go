package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type File struct {
	fileName      string
	columnNames   []string
	data          []interface{}
	sortBy        []string
	ignoreColumns []string
}

func NewFile(fn string, limit int, sortBy []string, ignoreColumns []string) (*File, error) {
	f := &File{
		fileName:      fn,
		sortBy:        sortBy,
		ignoreColumns: ignoreColumns,
	}
	fr, err := local.NewLocalFileReader(f.fileName)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, nil, 4)
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
	f.data, err = pr.ReadByNumber(l)
	if err != nil {
		return nil, err
	}
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
	return j.Encode(f.Data())
}

func (f *File) ToTable(w io.Writer) error {
	data := f.Data()
	tw := tabwriter.NewWriter(w, 0, 0, 0, '.', tabwriter.Debug)
	fmt.Fprintln(tw, strings.Join(f.columnNames, "\t"))
	for i := 0; i < len(data); i++ {
		res := []string{}
		row := data[i]
		for j := 0; j < len(f.columnNames); j++ {
			res = append(res, valToString(row[f.columnNames[j]]))
		}
		fmt.Fprintln(tw, strings.Join(res, "\t"))
	}
	return tw.Flush()
}

func (f *File) Data() []map[string]interface{} {
	ignoreKey := func(k string) bool {
		if arrayEmpty(f.ignoreColumns) {
			return false
		}
		for i := 0; i < len(f.ignoreColumns); i++ {
			ic := strings.ToLower(f.ignoreColumns[i])
			s := strings.ToLower(k)
			if s == ic {
				return true
			}
			if strings.HasPrefix(ic, "*") {
				if strings.HasSuffix(s, strings.Replace(ic, "*", "", 1)) {
					return true
				}
			}
			if strings.HasSuffix(ic, "*") {
				if strings.HasPrefix(s, strings.Replace(ic, "*", "", 1)) {
					return true
				}
			}
		}
		return false
	}
	b, err := json.Marshal(f.data)
	if err != nil {
		log.Fatal(err)
	}
	m := []map[string]interface{}{}
	err = json.Unmarshal(b, &m)
	if err != nil {
		log.Fatal(err)
	}
	if !arrayEmpty(f.sortBy) {
		sort.Slice(f.data, func(i, j int) bool {
			return getComposite(m[i], f.sortBy) < getComposite(m[j], f.sortBy)
		})
	}
	if arrayEmpty(f.ignoreColumns) {
		return m
	}
	m2 := []map[string]interface{}{}
	for i := 0; i < len(m); i++ {
		item := map[string]interface{}{}
		for k, v := range m2[i] {
			if !ignoreKey(k) {
				item[k] = v
			}
		}
		m2 = append(m2, item)
	}
	return m2
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
