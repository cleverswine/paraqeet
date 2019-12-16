package main

import (
	"fmt"
	"os"
	"text/tabwriter"
)

// Diff holds a diff for one record
type Diff struct {
	h  []string
	f1 []interface{}
	f2 []interface{}
	n  string
}

// NewDiff gets a new Diff
func NewDiff(n string) *Diff {
	return &Diff{n: n}
}

// Strings builds a string representation of the diff
func (d *Diff) String() {
	println("=====================================")
	println(d.n)
	println("=====================================")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.AlignRight|tabwriter.Debug)
	for i := 0; i < len(d.h); i++ {
		fmt.Fprintln(w, d.h[i]+"\t")
	}
	for i := 0; i < len(d.f1); i++ {
		fmt.Fprintln(w, valToString(d.f1[i])+"\t")
	}
	for i := 0; i < len(d.f2); i++ {
		fmt.Fprintln(w, valToString(d.f2[i])+"\t")
	}
	w.Flush()
}

// Add adds a record to the diff
func (d *Diff) Add(h string, f1 interface{}, f2 interface{}) {
	if d.h == nil {
		d.h = []string{}
	}
	if d.f1 == nil {
		d.f1 = []interface{}{}
	}
	if d.f2 == nil {
		d.f2 = []interface{}{}
	}
	d.h = append(d.h, h)
	d.f1 = append(d.f1, f1)
	d.f2 = append(d.f2, f2)
}

// Any checks if any diffs have been set for this record
func (d *Diff) Any() bool {
	return d.h != nil && len(d.h) > 0
}
