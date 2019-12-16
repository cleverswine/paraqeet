package main

import (
	"fmt"
	"io"
	"text/tabwriter"
)

// Diff represents a diff between 2 files
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

// Strings builds a string representation of the diff
func (d *Diff) String(out io.Writer) {
	println("\n=====================================")
	println(d.n)
	println("=====================================\n")
	w := tabwriter.NewWriter(out, 0, 0, 0, '.', tabwriter.Debug)
	res := "\t"
	for i := 0; i < len(d.h); i++ {
		res += d.h[i] + "\t"
	}
	fmt.Fprintln(w, res)
	res = "GOLD\t"
	for i := 0; i < len(d.f1); i++ {
		res += valToString(d.f1[i]) + "\t"
	}
	fmt.Fprintln(w, res)
	res = "COMP\t"
	for i := 0; i < len(d.f2); i++ {
		res += valToString(d.f2[i]) + "\t"
	}
	fmt.Fprintln(w, res)
	w.Flush()
}

// Differ performs a diff between 2 parquet scanners
type Differ struct {
	compareFile *Scanner
	goldFile    *Scanner
}

// NewDiffer gets a new differ
func NewDiffer(compareFile *Scanner, goldFile *Scanner) *Differ {
	return &Differ{compareFile: compareFile, goldFile: goldFile}
}

// Diff performs a diff between 2 parquet scanners
func (d *Differ) Diff(ignoreColumns []string, limit int) []*Diff {
	result := []*Diff{}

	ignore := func(s string) bool {
		if ignoreColumns == nil {
			return false
		}
		for i := 0; i < len(ignoreColumns); i++ {
			if s == ignoreColumns[i] {
				return true
			}
		}
		return false
	}

	for {
		if d.compareFile.EOF() && d.goldFile.EOF() {
			break
		}

		// gold records past compare
		if d.compareFile.EOF() {
			diff := NewDiff("line exists in gold file but not compare file")
			for k, v := range d.goldFile.CurrentLine() {
				diff.Add(k, v, nil)
			}
			result = append(result, diff)
			// continue scanning new file
			d.goldFile.Scan()
			continue
		}

		// compare records past gold
		if d.goldFile.EOF() {
			diff := NewDiff("line exists in compare file but not gold file")
			for k, v := range d.compareFile.CurrentLine() {
				diff.Add(k, v, nil)
			}
			result = append(result, diff)
			// continue scanning old file
			d.compareFile.Scan()
			continue
		}

		// same key so compare them
		if d.goldFile.Key() == d.compareFile.Key() {
			// get current line from each
			goldLine := d.goldFile.CurrentLine()
			compareLine := d.compareFile.CurrentLine()
			// find overlapping map keys
			mkeys := []string{}
			havemkey := func(s string) bool {
				for i := 0; i < len(mkeys); i++ {
					if mkeys[i] == s {
						return true
					}
				}
				return false
			}
			for k := range goldLine {
				if !ignore(k) {
					mkeys = append(mkeys, k)
				}
			}
			for k := range compareLine {
				if !ignore(k) && !havemkey(k) {
					mkeys = append(mkeys, k)
				}
			}
			// diff
			diff := NewDiff("Comparing: " + d.goldFile.Key())
			for i := 0; i < len(mkeys); i++ {
				k := mkeys[i]
				if goldLine[k] != compareLine[k] {
					diff.Add(k, goldLine[k], compareLine[k])
				}
			}
			if diff.Any() {
				result = append(result, diff)
			}
			// move to next line in both files
			d.compareFile.Scan()
			d.goldFile.Scan()
			continue
		}

		// old file has jumped past the new file
		if d.goldFile.Key() < d.compareFile.Key() {
			diff := NewDiff("line exists in compare file but not gold file")
			for k, v := range d.compareFile.CurrentLine() {
				diff.Add(k, v, nil)
			}
			result = append(result, diff)
			// continue scanning new file
			d.goldFile.Scan()
			continue
		}

		// new file has jumped past non-matching lines
		if d.goldFile.Key() > d.compareFile.Key() {
			diff := NewDiff("line exists in gold file but not compare file")
			for k, v := range d.goldFile.CurrentLine() {
				diff.Add(k, v, nil)
			}
			result = append(result, diff)
			// continue scanning old file
			d.compareFile.Scan()
			continue
		}

		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result
}
