package main

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
