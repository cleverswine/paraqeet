package main

import (
	"encoding/json"
	"strconv"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func valToString(i interface{}) string {
	if i == nil {
		return ""
	}
	switch v := i.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.Itoa(int(v))
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
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

func parquetToMap(fn string, limit int) ([]map[string]interface{}, error) {
	fr, err := local.NewLocalFileReader(fn)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()
	num := limit
	if num < 1 {
		num = int(pr.GetNumRows())
	}
	res, err := pr.ReadByNumber(num)
	if err != nil {
		return nil, err
	}
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
