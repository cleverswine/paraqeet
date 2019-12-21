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
	println("\n===============================================================================================================")
	println(d.n)
	println("===============================================================================================================")
	w := tabwriter.NewWriter(out, 0, 0, 0, '.', tabwriter.Debug)
	res := "\t"
	for i := 0; i < len(d.h); i++ {
		res += d.h[i] + "\t"
	}
	fmt.Fprintln(w, res)
	res = "COMP File\t"
	for i := 0; i < len(d.f1); i++ {
		res += valToString(d.f1[i]) + "\t"
	}
	fmt.Fprintln(w, res)
	res = "GOLD File\t"
	for i := 0; i < len(d.f2); i++ {
		res += valToString(d.f2[i]) + "\t"
	}
	fmt.Fprintln(w, res)
	w.Flush()
}

type Differ struct {
	f1         *File
	f2         *File
	limit      int
	keyColumns []string
}

func NewDiffer(f1 *File, f2 *File, limit int, keyColumns []string) *Differ {
	return &Differ{
		f1: f1, f2: f2, limit: limit, keyColumns: keyColumns,
	}
}

func (d *Differ) Diff() []*Diff {
	result := []*Diff{}
	f1Data := d.f1.GetAllData()
	f1Index := 0
	f2Data := d.f2.GetAllData()
	f2Index := 0
	eof := func(i int, t int) bool {
		return i+1 > t
	}
	for {
		// don't go past the limited number of results
		if d.limit > 0 && len(result) >= d.limit {
			break
		}
		// reached eof on both
		if eof(f1Index, len(f1Data)) && eof(f2Index, len(f2Data)) {
			break
		}
		// f2 records past f1 eof
		if eof(f1Index, len(f1Data)) {
			diff := NewDiff("line exists in gold file but not compare file")
			for i := 0; i < len(d.keyColumns); i++ {
				diff.Add(d.keyColumns[i], nil, f2Data[f2Index][d.keyColumns[i]])
			}
			result = append(result, diff)
			// continue scanning f2
			f2Index++
			continue
		}
		// f1 records past f2 eof
		if eof(f2Index, len(f2Data)) {
			diff := NewDiff("line exists in gold file but not compare file")
			for i := 0; i < len(d.keyColumns); i++ {
				diff.Add(d.keyColumns[i], f1Data[f1Index][d.keyColumns[i]], nil)
			}
			result = append(result, diff)
			// continue scanning f1
			f1Index++
			continue
		}
		f1DataKey := d.f1.GetCompositeFromMap(f1Data[f1Index], d.keyColumns)
		f2DataKey := d.f2.GetCompositeFromMap(f2Data[f2Index], d.keyColumns)
		// same key, do a compare
		if f1DataKey == f2DataKey {
			diff := d.diffRow(f1Data[f1Index], f2Data[f2Index])
			if diff != nil {
				result = append(result, diff)
			}
			f1Index++
			f2Index++
			continue
		}
		// f1 file has jumped past f2 file
		if f2DataKey < f1DataKey {
			diff := NewDiff("line exists in f1 file but not f2 file")
			for i := 0; i < len(d.keyColumns); i++ {
				diff.Add(d.keyColumns[i], f1Data[f1Index][d.keyColumns[i]], nil)
			}
			result = append(result, diff)
			// continue scanning f2
			f2Index++
			continue
		}
		// f2 file has jumped past f1 file
		if f1DataKey < f2DataKey {
			diff := NewDiff("line exists in f2 file but not f1 file")
			for i := 0; i < len(d.keyColumns); i++ {
				diff.Add(d.keyColumns[i], nil, f2Data[f2Index][d.keyColumns[i]])
			}
			result = append(result, diff)
			// continue scanning f1
			f1Index++
			continue
		}
	}
	return result
}

func (d *Differ) diffRow(r1 map[string]interface{}, r2 map[string]interface{}) *Diff {
	// find overlapping map keys
	keysToCompare := []string{}
	keyAdded := func(s string) bool {
		for i := 0; i < len(keysToCompare); i++ {
			if keysToCompare[i] == s {
				return true
			}
		}
		return false
	}
	for k := range r1 {
		keysToCompare = append(keysToCompare, k)
	}
	for k := range r2 {
		if !keyAdded(k) {
			keysToCompare = append(keysToCompare, k)
		}
	}
	// compare
	diffs := map[string][]interface{}{}
	for i := 0; i < len(keysToCompare); i++ {
		k := keysToCompare[i]
		if valToString(r1[k]) != valToString(r2[k]) {
			diffs[k] = []interface{}{r1[k], r2[k]}
		}
	}
	if len(diffs) == 0 {
		return nil
	}
	// add in key values for reference
	for i := 0; i < len(d.keyColumns); i++ {
		k := d.keyColumns[i]
		diffs[k] = []interface{}{r1[k], r2[k]}
	}

	diff := NewDiff("Row has differences")
	// add diffs in order
	// for i := 0; i < len(d.f1.ColumnNames()); i++ {
	// 	k := d.f1.ColumnNames()[i]
	// 	if d, ok := diffs[k]; ok {
	// 		diff.Add(k, d[0], d[1])
	// 	}
	// }
	return diff
}
