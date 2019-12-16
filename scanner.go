package main

import (
	"io"
	"sort"
)

const keyName = "paraqeet_key"
const sortName = "paraqeet_sort"

// Scanner allows for scanning a dataset row by row
type Scanner struct {
	data      []map[string]interface{}
	numRows   int
	currIndex int
}

// NewScannerFromParquetFile generates a new ParquetScanner from a parquet file name
func NewScannerFromParquetFile(fn string, keyColumns []string, sortColumns []string, limit int) (*Scanner, error) {
	data, err := parquetToMap(fn, limit)
	if err != nil {
		return nil, err
	}
	return NewScanner(data, keyColumns, sortColumns), nil
}

// NewScanner generates a new ParquetScanner
func NewScanner(data []map[string]interface{}, keyColumns []string, sortColumns []string) *Scanner {
	// add a key and sort property to each item
	if sortColumns == nil || len(sortColumns) == 0 {
		sortColumns = keyColumns
	}
	for i := 0; i < len(data); i++ {
		data[i][keyName] = getComposite(data[i], keyColumns)
		data[i][sortName] = getComposite(data[i], sortColumns)
	}
	// sort on sort columns
	sort.Slice(data, func(i, j int) bool {
		return data[i][sortName].(string) > data[j][sortName].(string)
	})
	return &Scanner{
		data:    data,
		numRows: len(data),
	}
}

// Scan reads in the next line
func (scanner *Scanner) Scan() error {
	if scanner.currIndex >= scanner.numRows {
		// caller should check for EOF before scanning, so return an actual EOF error here
		return io.EOF
	}
	scanner.currIndex++
	return nil
}

// EOF checks if we're at the end of the dataset
func (scanner *Scanner) EOF() bool {
	return scanner.currIndex >= scanner.numRows
}

// Key returns the composite key of the current line
func (scanner *Scanner) Key() string {
	return scanner.getVal(keyName).(string)
}

// CurrentLine gets the current line of the scanner
func (scanner *Scanner) CurrentLine() map[string]interface{} {
	return scanner.data[scanner.currIndex]
}

// CurrentLine gets the current line of the scanner
func (scanner *Scanner) String() string {
	result := ""
	for k, v := range scanner.CurrentLine() {
		result += k + ": " + valToString(v) + " || "
	}
	return result
}

func (scanner *Scanner) getVal(k string) interface{} {
	return scanner.CurrentLine()[k]
}
