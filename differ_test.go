package main

import (
	"testing"
)

func TestDiffer_Diff(t *testing.T) {
	sc1, err := NewScannerFromParquetFile("sample_files/path1/columnMismatch.parquet", []string{"MessageId"}, nil)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	sc2, err := NewScannerFromParquetFile("sample_files/path2/columnMismatch.parquet", []string{"MessageId"}, nil)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	differ := NewDiffer(sc1, sc2)
	result := differ.Diff(nil, -1)
	for i := 0; i < len(result); i++ {
		result[i].String()
	}
}
