package main

import (
	"testing"
)

func TestNewScannerFromParquetFile(t *testing.T) {
	sc, err := NewScannerFromParquetFile("sample_files/20190408_newrxmetrics_v6_0.parquet", []string{"MessageId"}, nil, 10)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	for {
		if sc.CurrentLine()["AlienPhrase"] != nil {
			//println(sc.String())
			break
		}
		if sc.EOF() {
			break
		}
		sc.Scan()
	}
}

func TestNewScanner(t *testing.T) {

}

func TestScanner_Scan(t *testing.T) {

}

func TestScanner_EOF(t *testing.T) {

}

func TestScanner_Key(t *testing.T) {

}

func TestScanner_CurrentLine(t *testing.T) {

}
