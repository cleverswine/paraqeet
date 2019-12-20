package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

var o string

func main() {
	//pq, err := LoadParaqeetFile("../../cpi/calculateduplicatemessages/hdfs/data/auddm/tables/message/dt=2019-12-02/20191202_message.parquet", []string{"*ID"}, -1)
	pq, err := LoadParaqeetFile("sample_files/columnMismatch1.parquet", nil, -1)
	if err != nil {
		log.Fatalln(err.Error())
	}
	json.NewEncoder(os.Stdout).Encode(pq.GetRow(0))
	// pq.Sort([]string{"first_name", "last_name"})
	// jd.Encode(pq)
	// return
	// var rootCmd = &cobra.Command{Use: "paraqeet"}
	// rootCmd.PersistentFlags().StringVarP(&o, "output", "o", "", "output file for the results (defaults to standard out)")
	// rootCmd.AddCommand(cmdDiff(), cmdInfo(), cmdCat())
	// if err := rootCmd.Execute(); err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }
}

type ParaqeetSchema struct {
	Name string
	Type string
}

type ParaqeetFile struct {
	Schema          []ParaqeetSchema
	SchemaNamespace string
	TotalRowCount   int
	data            [][]interface{}
}

func (f *ParaqeetFile) AddData(data []interface{}) {
	if f.data == nil {
		f.data = [][]interface{}{}
	}
	f.data = append(f.data, data)
}

func (f *ParaqeetFile) GetAllData() []map[string]interface{} {
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

func (f *ParaqeetFile) GetRow(index int) map[string]interface{} {
	result := map[string]interface{}{}
	for i := 0; i < len(f.Schema); i++ {
		result[f.Schema[i].Name] = f.data[index][i]
	}
	return result
}

func (f *ParaqeetFile) Sort(sortBy []string) {
	arrayEmpty := func(a []string) bool {
		return a == nil || len(a) == 0 || a[0] == ""
	}
	if arrayEmpty(sortBy) {
		return
	}
	getColumn := func(colName string) (*ParaqeetSchema, int) {
		for i := 0; i < len(f.Schema); i++ {
			if strings.ToLower(f.Schema[i].Name) == strings.ToLower(colName) {
				return &f.Schema[i], i
			}
		}
		return nil, -1
	}
	toString := func(sc *ParaqeetSchema, val interface{}) string {
		switch sc.Type {
		case "BYTE_ARRAY":
			return val.(string)
		case "INT64":
			return strconv.FormatInt(val.(int64), 10)
		default:
			b, _ := json.Marshal(val)
			return string(b)
		}
	}
	getComposite := func(row []interface{}) string {
		result := ""
		for i := 0; i < len(sortBy); i++ {
			sc, index := getColumn(sortBy[i])
			if sc != nil {
				result += toString(sc, row[index])
			}
		}
		return result
	}
	sort.Slice(f.data, func(i, j int) bool {
		return getComposite(f.data[i]) < getComposite(f.data[j])
	})
}

func LoadParaqeetFile(fn string, ignore []string, limit int) (*ParaqeetFile, error) {
	arrayEmpty := func(a []string) bool {
		return a == nil || len(a) == 0 || a[0] == ""
	}
	ignoreColumn := func(k string) bool {
		if arrayEmpty(ignore) {
			return false
		}
		for i := 0; i < len(ignore); i++ {
			ic := strings.ToLower(ignore[i])
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
	pqSchema := pr.Footer.GetSchema()
	// populate return val with some info
	pq := &ParaqeetFile{
		TotalRowCount:   int(pr.Footer.GetNumRows()),
		Schema:          []ParaqeetSchema{},
		SchemaNamespace: pqSchema[0].Name,
		data:            [][]interface{}{},
	}
	// try reading the data for each column
	l := limit
	if l < 1 || l > pq.TotalRowCount {
		l = pq.TotalRowCount
	}
	data := map[string][]interface{}{}
	for i := 1; i < len(pqSchema); i++ {
		sc := pqSchema[i]
		if ignoreColumn(sc.Name) {
			continue
		}
		t := sc.GetType()
		vals, _, _, err := pr.ReadColumnByPath(pq.SchemaNamespace+"."+sc.Name, l)
		if err == nil {
			data[sc.Name] = vals
			pq.Schema = append(pq.Schema, ParaqeetSchema{Name: sc.Name, Type: t.String()})
		} else {
			fmt.Println(err)
		}
	}
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
