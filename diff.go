package main

import (
	"fmt"
	"io"
	"text/tabwriter"
)

// Diff represents a diff between 2 rows of data
type Diff struct {
	columns  []string
	f1Values []interface{}
	f2Values []interface{}
	note     string
}

// NewDiff starts a new row Diff with the specified note
func NewDiff(n string) *Diff {
	return &Diff{note: n}
}

// Add adds an item to the diff
func (d *Diff) Add(h string, f1 interface{}, f2 interface{}) {
	if d.columns == nil {
		d.columns = []string{}
	}
	if d.f1Values == nil {
		d.f1Values = []interface{}{}
	}
	if d.f2Values == nil {
		d.f2Values = []interface{}{}
	}
	d.columns = append(d.columns, h)
	d.f1Values = append(d.f1Values, f1)
	d.f2Values = append(d.f2Values, f2)
}

// Any checks if any items have been set for this Diff
func (d *Diff) Any() bool {
	return d.columns != nil && len(d.columns) > 0
}

// String builds a string representation of the diff
func (d *Diff) String(out io.Writer) {
	println("\n===============================================================================================================")
	println(d.note)
	println("===============================================================================================================")
	w := tabwriter.NewWriter(out, 0, 0, 0, '.', tabwriter.Debug)
	res := "\t"
	for i := 0; i < len(d.columns); i++ {
		res += d.columns[i] + "\t"
	}
	fmt.Fprintln(w, res)
	res = "COMP File\t"
	for i := 0; i < len(d.f1Values); i++ {
		res += toString(d.f1Values[i]) + "\t"
	}
	fmt.Fprintln(w, res)
	res = "GOLD File\t"
	for i := 0; i < len(d.f2Values); i++ {
		res += toString(d.f2Values[i]) + "\t"
	}
	fmt.Fprintln(w, res)
	w.Flush()
}

// Differ performs a diff between two Files
type Differ struct {
	f1         *File
	f2         *File
	limit      int
	keyColumns []string
}

// NewDiffer creates a new Differ
func NewDiffer(f1 *File, f2 *File, limit int, keyColumns []string) *Differ {
	return &Differ{
		f1: f1, f2: f2, limit: limit, keyColumns: keyColumns,
	}
}

// Diff performs the diff, returning an array of row Diffs
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
		if toString(r1[k]) != toString(r2[k]) {
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
	// add diffs in column order
	for i := 0; i < len(d.f1.Columns()); i++ {
		k := d.f1.Columns()[i]
		if d, ok := diffs[k]; ok {
			diff.Add(k, d[0], d[1])
		}
	}
	return diff
}
