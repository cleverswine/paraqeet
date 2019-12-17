package main

import (
	"os"
	"testing"
)

func TestNewParaqeet(t *testing.T) {
	f, err := NewParaqeet("sample_files/example.parquet")
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer f.Close()
	f.Info(os.Stdout)
	f.ToJson(10, os.Stdout)
}
