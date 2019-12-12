package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// Differ performs a diff between 2 parquet scanners
type Differ struct {
	compareFile *ParquetScanner
	goldFile    *ParquetScanner
}

// NewDiffer gets a new differ
func NewDiffer(compareFile *ParquetScanner, goldFile *ParquetScanner) *Differ {
	return &Differ{compareFile: compareFile, goldFile: goldFile}
}

// Diff performs a diff between 2 parquet scanners
func (d *Differ) Diff(w io.Writer) {
	wlog := func(s string) {
		w.Write([]byte(s))
	}

	// scan first data row
	d.compareFile.Scan()
	d.goldFile.Scan()

	for {
		if d.compareFile.EOF() && d.goldFile.EOF() {
			break
		}

		// gold records past compare
		if d.compareFile.EOF() {
			wlog(fmt.Sprintf("%s in gold but not in compare\n", d.goldFile.String()))
			// continue scanning new file
			d.goldFile.Scan()
			continue
		}

		// compare records past gold
		if d.goldFile.EOF() {
			wlog(fmt.Sprintf("%s in compare but not in gold\n", d.compareFile.String()))
			// continue scanning old file
			d.compareFile.Scan()
			continue
		}

		// duplicate rows - NOOP
		if d.goldFile.Hash() == d.compareFile.Hash() {
			// move to next line in both files
			d.compareFile.Scan()
			d.goldFile.Scan()
			continue
		}

		// same key, different hash
		if d.goldFile.Key() == d.compareFile.Key() {
			wlog(fmt.Sprintf("compare not same as gold \n%s\n%s\n", d.compareFile.String(), d.goldFile.String()))
			// move to next line in both files
			d.compareFile.Scan()
			d.goldFile.Scan()
			continue
		}

		// old file has jumped past the new file
		if d.goldFile.Key() < d.compareFile.Key() {
			wlog(fmt.Sprintf("%s in compare but not in gold\n", d.compareFile.String()))
			// continue scanning new file
			d.goldFile.Scan()
			continue
		}

		// new file has jumped past non-matching lines
		if d.goldFile.Key() > d.compareFile.Key() {
			wlog(fmt.Sprintf("%s in gold but not in compare\n", d.goldFile.String()))
			// continue scanning old file
			d.compareFile.Scan()
			continue
		}
	}
}

// ParquetScanner allows for reading a dataset row by row
type ParquetScanner struct {
	data       []map[string]interface{}
	keyColumns []string
	numRows    int
	currIndex  int
}

// NewParquetScanner generates a new ParquetScanner
func NewParquetScanner(data []map[string]interface{}, keyColumns []string) *ParquetScanner {
	return &ParquetScanner{
		data:       data,
		keyColumns: keyColumns,
		numRows:    len(data),
	}
}

// Scan reads in the next line
func (scanner *ParquetScanner) Scan() error {
	if scanner.currIndex >= scanner.numRows {
		// caller should check for EOF before scanning, so return an actual EOF error here
		return io.EOF
	}
	scanner.currIndex++
	return nil
}

// EOF checks if we're at the end of the dataset
func (scanner *ParquetScanner) EOF() bool {
	return scanner.currIndex >= scanner.numRows
}

// Key returns the composite key of the current line
func (scanner *ParquetScanner) Key() string {
	result := ""
	for i := 0; i < len(scanner.keyColumns); i++ {
		result = result + scanner.valToString(scanner.getVal(scanner.keyColumns[i]))
	}
	return result
}

// Hash returns the hash of the current line
func (scanner *ParquetScanner) Hash() string {
	return scanner.String()
}

// String returns a string representation of the current line
func (scanner *ParquetScanner) String() string {
	result := ""
	for k, v := range scanner.currentLine() {
		result = result + fmt.Sprintf("%s: %s", k, scanner.valToString(v))
	}
	return result
}

func (scanner *ParquetScanner) getVal(k string) interface{} {
	return scanner.currentLine()[k]
}

func (scanner *ParquetScanner) currentLine() map[string]interface{} {
	return scanner.data[scanner.currIndex]
}

func (scanner *ParquetScanner) valToString(i interface{}) string {
	switch v := i.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.Itoa(int(v))
	}
	return fmt.Sprintf("?: %t", i)
}

func main() {
	fr, err := local.NewLocalFileReader("sample_files/20190408_newrxmetrics_v6_0.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return
	}

	b, err := json.Marshal(res)
	if err != nil {
		log.Println("Can't marshal", err)
		return
	}
	// println(string(b))
	//ioutil.WriteFile("o.json", b, os.ModePerm)

	/*
	   "AlienPhrase": {
	       "Map": [
	           {
	               "Key": "sig",
	               "Value": {
	                   "Bag": [
	                       {
	                           "Array": "by mouth"
	                       },
	                       {
	                           "Array": "take"
	                       }
	                   ]
	               }
	           },
	           {
	               "Key": "coupon",
	               "Value": {
	                   "Bag": [
	                       {
	                           "Array": "bin"
	                       }
	                   ]
	               }
	           }
	       ]
	   },
	*/
	m := []map[string]interface{}{}
	err = json.Unmarshal(b, &m)
	if err != nil {
		log.Println("Can't unmarshal", err)
		return
	}
	println(m[0]["MessageId"].(float64))
}
