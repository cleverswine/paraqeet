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

type Schema struct {
	Name string
	Path string
}

type File struct {
	Schema         []Schema
	TotalRowCount  int
	LoadedRowCount int
	data           [][]interface{}
}

func (f *File) ToTable(out io.Writer) error {
	tw := tabwriter.NewWriter(out, 0, 0, 0, '.', tabwriter.Debug)
	columnNames := []string{}
	for _, sc := range f.Schema {
		columnNames = append(columnNames, sc.Name)
	}
	fmt.Fprintln(tw, strings.Join(columnNames, "\t"))
	for i := 0; i < len(f.data); i++ {
		fmt.Fprintln(tw, strings.Join(f.GetRowAsStrings(i), "\t"))
	}
	return tw.Flush()
}

func (f *File) ToJson(out io.Writer) error {
	return json.NewEncoder(out).Encode(f.GetAllData())
}

func (f *File) AddData(data []interface{}) {
	if f.data == nil {
		f.data = [][]interface{}{}
	}
	f.data = append(f.data, data)
}

func (f *File) GetAllData() []map[string]interface{} {
	result := []map[string]interface{}{}
	for i := 0; i < len(f.data); i++ {
		item := map[string]interface{}{}
		for j := 0; j < len(f.Schema); j++ {
			item[f.Schema[j].Name] = f.data[i][j]
		}
		result = append(result, item)
	}
	return result
}

func (f *File) GetRowAsStrings(index int) []string {
	result := []string{}
	for i := 0; i < len(f.Schema); i++ {
		result = append(result, toString(f.data[index][i]))
	}
	return result
}

func (f *File) GetRow(index int) map[string]interface{} {
	result := map[string]interface{}{}
	for i := 0; i < len(f.Schema); i++ {
		result[f.Schema[i].Name] = f.data[index][i]
	}
	return result
}

func (f *File) Sort(sortBy []string) {
	arrayEmpty := func(a []string) bool {
		return a == nil || len(a) == 0 || a[0] == ""
	}
	if arrayEmpty(sortBy) {
		return
	}
	getColumn := func(colName string) (*Schema, int) {
		for i := 0; i < len(f.Schema); i++ {
			if strings.ToLower(f.Schema[i].Name) == strings.ToLower(colName) {
				return &f.Schema[i], i
			}
		}
		return nil, -1
	}
	getComposite := func(row []interface{}) string {
		result := ""
		for i := 0; i < len(sortBy); i++ {
			sc, index := getColumn(sortBy[i])
			if sc != nil {
				result += toString(row[index])
			}
		}
		return result
	}
	sort.Slice(f.data, func(i, j int) bool {
		return getComposite(f.data[i]) < getComposite(f.data[j])
	})
}

func LoadFile(fn string, ignore []string, restrict []string, limit int) (*File, error) {
	arrayEmpty := func(a []string) bool {
		return a == nil || len(a) == 0 || a[0] == ""
	}
	contains := func(arr []string, k string) bool {
		for i := 0; i < len(arr); i++ {
			ic := strings.ToLower(arr[i])
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
	ignoreColumn := func(k string) bool {
		if !arrayEmpty(restrict) {
			return !contains(restrict, k)
		}
		if !arrayEmpty(ignore) {
			return contains(ignore, k)
		}
		return false
	}
	fr, err := local.NewLocalFileReader(fn)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	pr, err := reader.NewParquetColumnReader(fr, 4)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()
	// get some parquet file info
	err = pr.ReadFooter()
	if err != nil {
		return nil, err
	}
	//pqSchema := pr.Footer.GetSchema()
	//json.NewEncoder(os.Stdout).Encode(pr.SchemaHandler.IndexMap)
	// populate return val with some info
	t := int(pr.Footer.GetNumRows())
	l := limit
	if l < 1 || l > t {
		l = t
	}
	pq := &File{
		TotalRowCount:  t,
		LoadedRowCount: l,
		Schema:         []Schema{},
		data:           [][]interface{}{},
	}
	// try reading the data for each column
	data := map[string][]interface{}{}
	for i := 0; i < len(pr.SchemaHandler.IndexMap); i++ {
		col := pr.SchemaHandler.IndexMap[int32(i)]
		ns := strings.Split(col, ".")
		colName := ns[len(ns)-1]
		if len(ns) > 2 || len(ns) == 0 {
			continue
		}
		if ignoreColumn(colName) {
			continue
		}
		vals, _, _, err := pr.ReadColumnByPath(col, 10)
		if err == nil {
			data[colName] = vals
			pq.Schema = append(pq.Schema, Schema{Name: colName, Path: col})
		} else {
			// log.debug...
			fmt.Println(err)
		}
	}
	// for i := 1; i < len(pqSchema); i++ {
	// 	sc := pqSchema[i]
	// 	if ignoreColumn(sc.Name) {
	// 		continue
	// 	}
	// 	t := sc.GetType()
	// 	vals, _, _, err := pr.ReadColumnByPath(pq.SchemaNamespace+"."+sc.Name, l)
	// 	if err == nil {
	// 		data[sc.Name] = vals
	// 		pq.Schema = append(pq.Schema, ParaqeetSchema{Name: sc.Name, Type: t.String()})
	// 	} else {
	// 		// log.debug...
	// 		//fmt.Println(err)
	// 	}
	// }
	// convert the columner data to []rows
	for i := 0; i < l; i++ {
		result := []interface{}{}
		// columns
		for j := 0; j < len(pq.Schema); j++ {
			row := data[pq.Schema[j].Name]
			if len(row) > i {
				result = append(result, row[i])
			}
		}
		pq.AddData(result)
	}
	return pq, nil
}
