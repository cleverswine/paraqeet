package main

import (
	"os"
	"testing"
)

func TestDiffer_Diff(t *testing.T) {
	f1, err := NewParaqeet("sample_files/columnMismatch1.parquet")
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer f1.Close()
	f2, err := NewParaqeet("sample_files/columnMismatch2.parquet")
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer f2.Close()

	differ := NewDiffer(f1, f2, 100, []string{"MessageId"}, nil, nil)
	result := differ.Diff()
	for i := 0; i < len(result); i++ {
		result[i].String(os.Stdout)
	}
}
