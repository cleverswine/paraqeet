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

// File represents a column-based file
type File struct {
	ColumnsByIndex map[int]string
	ColumnsByName  map[string]int
	TotalRowCount  int
	LoadedRowCount int
	data           [][]interface{}
}

// Columns returns the names of the columns in order
func (f *File) Columns() []string {
	cols := []string{}
	for i := 0; i < len(f.ColumnsByIndex); i++ {
		cols = append(cols, f.ColumnsByIndex[i])
	}
	return cols
}

// Info writes out info about the File
func (f *File) Info(out io.Writer) {
	fmt.Fprintf(out, "\nColumns: \n%s\n\nRow Count: %d\n\n", strings.Join(f.Columns(), " | "), f.TotalRowCount)
}

// ToTable writes a table-formatted representation of the file
func (f *File) ToTable(out io.Writer) error {
	tw := tabwriter.NewWriter(out, 0, 0, 0, '.', tabwriter.Debug)
	columnNames := []string{}
	for i := 0; i < len(f.ColumnsByIndex); i++ {
		columnNames = append(columnNames, f.ColumnsByIndex[i])
	}
	fmt.Fprintln(tw, strings.Join(columnNames, "\t"))
	for i := 0; i < len(f.data); i++ {
		fmt.Fprintln(tw, strings.Join(f.GetRowAsStrings(i), "\t"))
	}
	return tw.Flush()
}

// ToJSON writes a json-formatted representation of the file
func (f *File) ToJSON(out io.Writer) error {
	return json.NewEncoder(out).Encode(f.GetAllData())
}

// AddData adds a row of data
func (f *File) AddData(data []interface{}) {
	if f.data == nil {
		f.data = [][]interface{}{}
	}
	f.data = append(f.data, data)
}

// GetAllData returns all data, with each row represented as a Map
func (f *File) GetAllData() []map[string]interface{} {
	result := []map[string]interface{}{}
	for i := 0; i < len(f.data); i++ {
		item := map[string]interface{}{}
		for j := 0; j < len(f.ColumnsByIndex); j++ {
			item[f.ColumnsByIndex[j]] = f.data[i][j]
		}
		result = append(result, item)
	}
	return result
}

// GetRowAsStrings gets a single row with all values converted to string
func (f *File) GetRowAsStrings(index int) []string {
	result := []string{}
	for i := 0; i < len(f.ColumnsByIndex); i++ {
		result = append(result, toString(f.data[index][i]))
	}
	return result
}

// GetRow gets a single row represented as a Map
func (f *File) GetRow(index int) map[string]interface{} {
	result := map[string]interface{}{}
	for i := 0; i < len(f.ColumnsByIndex); i++ {
		result[f.ColumnsByIndex[i]] = f.data[index][i]
	}
	return result
}

// GetComposite combines multiple columns of a row into one string (for composite keys, etc)
func (f *File) GetComposite(row []interface{}, cols []string) string {
	result := ""
	for i := 0; i < len(cols); i++ {
		if colIndex, ok := f.ColumnsByName[cols[i]]; ok {
			result += toString(row[colIndex])
		}
	}
	return result
}

// GetCompositeFromMap combines multiple columns of a row into one string (for composite keys, etc)
func (f *File) GetCompositeFromMap(row map[string]interface{}, cols []string) string {
	result := ""
	for i := 0; i < len(cols); i++ {
		if rowItem, ok := row[cols[i]]; ok {
			result += toString(rowItem)
		}
	}
	return result
}

// Sort sorts the file in place
func (f *File) Sort(sortBy []string) {
	arrayEmpty := func(a []string) bool {
		return a == nil || len(a) == 0 || a[0] == ""
	}
	if arrayEmpty(sortBy) {
		return
	}
	sort.Slice(f.data, func(i, j int) bool {
		return f.GetComposite(f.data[i], sortBy) < f.GetComposite(f.data[j], sortBy)
	})
}

// LoadFile loads a File from a parquet file
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
		ColumnsByIndex: map[int]string{},
		ColumnsByName:  map[string]int{},
		data:           [][]interface{}{},
	}
	// try reading the data for each column
	data := map[string][]interface{}{}
	j := 0
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
		vals, _, _, err := pr.ReadColumnByPath(col, l)
		if err == nil {
			data[colName] = vals
			pq.ColumnsByIndex[j] = colName
			pq.ColumnsByName[colName] = j
			j++
		} else {
			// log.debug...
			//fmt.Println(err)
		}
	}
	// convert the columner data to []rows
	for i := 0; i < l; i++ {
		result := []interface{}{}
		// columns
		for j := 0; j < len(pq.ColumnsByIndex); j++ {
			row := data[pq.ColumnsByIndex[j]]
			if len(row) > i {
				result = append(result, row[i])
			}
		}
		pq.AddData(result)
	}
	return pq, nil
}
