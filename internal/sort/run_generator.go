package sortpkg

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
)

// External run generation optimized with byte-level parsing.
// Each run file stores raw lines (with newline).

type RunGenerator struct {
	MemLimitRecords int
	RunDir          string
	runSeq          uint64
}

func NewRunGenerator(runDir string, memLimitPerBuf int) *RunGenerator {
	_ = os.MkdirAll(runDir, 0755)
	return &RunGenerator{
		MemLimitRecords: memLimitPerBuf,
		RunDir:          runDir,
	}
}

// GenerateRuns consumes CSV lines from channel and produces sorted run files for each key.
// Returns lists of run file paths.
func (rg *RunGenerator) GenerateRuns(lines <-chan string) (idRuns, nameRuns, continentRuns []string, err error) {
	idBuf := make([][]byte, 0, rg.MemLimitRecords)
	nameBuf := make([][]byte, 0, rg.MemLimitRecords)
	contBuf := make([][]byte, 0, rg.MemLimitRecords)

	flush := func(buf [][]byte, key string) (string, error) {
		if len(buf) == 0 {
			return "", nil
		}
		seq := atomic.AddUint64(&rg.runSeq, 1)
		fn := filepath.Join(rg.RunDir, fmt.Sprintf("%s.run.%d.tmp", key, seq))
		f, er := os.Create(fn)
		if er != nil {
			return "", er
		}
		w := bufio.NewWriterSize(f, 1<<20)
		for _, l := range buf {
			if _, er = w.Write(l); er != nil {
				_ = f.Close()
				return "", er
			}
		}
		if er = w.Flush(); er != nil {
			_ = f.Close()
			return "", er
		}
		_ = f.Close()
		return fn, nil
	}

	// comparator functions use fast byte-level comparisons
	sortByID := func(slice [][]byte) {
		sort.Slice(slice, func(i, j int) bool {
			// numeric compare until first comma
			li := slice[i]
			lj := slice[j]
			// parse integer quickly
			pi := parseIntPrefix(li)
			pj := parseIntPrefix(lj)
			return pi < pj
		})
	}
	sortByName := func(slice [][]byte) {
		sort.Slice(slice, func(i, j int) bool {
			li := slice[i]
			lj := slice[j]
			si, ei := extractNameBounds(li)
			sj, ej := extractNameBounds(lj)
			// compare bytes
			return string(li[si:ei]) < string(lj[sj:ej])
		})
	}
	sortByCont := func(slice [][]byte) {
		sort.Slice(slice, func(i, j int) bool {
			li := slice[i]
			lj := slice[j]
			si, ei := extractContBounds(li)
			sj, ej := extractContBounds(lj)
			return string(li[si:ei]) < string(lj[sj:ej])
		})
	}

	for l := range lines {
		// convert to []byte once (including newline)
		b := []byte(l)
		// store copies to avoid slice aliasing if caller reuses buffer
		// make copy
		cp := make([]byte, len(b))
		copy(cp, b)
		idBuf = append(idBuf, cp)
		nameBuf = append(nameBuf, cp)
		contBuf = append(contBuf, cp)

		if len(idBuf) >= rg.MemLimitRecords {
			sortByID(idBuf)
			fn, er := flush(idBuf, "id")
			if er != nil {
				return nil, nil, nil, er
			}
			if fn != "" {
				idRuns = append(idRuns, fn)
			}
			idBuf = idBuf[:0]
		}
		if len(nameBuf) >= rg.MemLimitRecords {
			sortByName(nameBuf)
			fn, er := flush(nameBuf, "name")
			if er != nil {
				return nil, nil, nil, er
			}
			if fn != "" {
				nameRuns = append(nameRuns, fn)
			}
			nameBuf = nameBuf[:0]
		}
		if len(contBuf) >= rg.MemLimitRecords {
			sortByCont(contBuf)
			fn, er := flush(contBuf, "continent")
			if er != nil {
				return nil, nil, nil, er
			}
			if fn != "" {
				continentRuns = append(continentRuns, fn)
			}
			contBuf = contBuf[:0]
		}
	}

	// flush remaining buffers
	if len(idBuf) > 0 {
		sortByID(idBuf)
		fn, er := flush(idBuf, "id")
		if er != nil {
			return nil, nil, nil, er
		}
		if fn != "" {
			idRuns = append(idRuns, fn)
		}
	}
	if len(nameBuf) > 0 {
		sortByName(nameBuf)
		fn, er := flush(nameBuf, "name")
		if er != nil {
			return nil, nil, nil, er
		}
		if fn != "" {
			nameRuns = append(nameRuns, fn)
		}
	}
	if len(contBuf) > 0 {
		sortByCont(contBuf)
		fn, er := flush(contBuf, "continent")
		if er != nil {
			return nil, nil, nil, er
		}
		if fn != "" {
			continentRuns = append(continentRuns, fn)
		}
	}
	return idRuns, nameRuns, continentRuns, nil
}

// helper: parse integer prefix until comma
func parseIntPrefix(b []byte) int64 {
	var v int64
	sign := int64(1)
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c == '-' {
			sign = -1
			continue
		}
		if c == ',' {
			break
		}
		v = v*10 + int64(c-'0')
	}
	return v * sign
}

// helper for name bounds
func extractNameBounds(b []byte) (int, int) {
	n := len(b)
	i := 0
	for i < n && b[i] != ',' {
		i++
	}
	start := i + 1
	j := start
	for j < n && b[j] != ',' {
		j++
	}
	return start, j
}

// helper for continent bounds (last field)
func extractContBounds(b []byte) (int, int) {
	n := len(b)
	// ignore trailing newline
	if n > 0 && b[n-1] == '\n' {
		n--
	}
	i := n - 1
	for i >= 0 && b[i] != ',' {
		i--
	}
	start := i + 1
	return start, n
}
