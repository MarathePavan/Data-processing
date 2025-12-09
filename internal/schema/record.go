package schema

import (
	"bytes"
	"strconv"
)

type Record struct {
	ID        int32
	Name      string
	Address   string
	Continent string
}

func (r *Record) ToCSV() []byte {
	var b bytes.Buffer
	b.WriteString(strconv.Itoa(int(r.ID)))
	b.WriteByte(',')
	b.WriteString(r.Name)
	b.WriteByte(',')
	b.WriteString(r.Address)
	b.WriteByte(',')
	b.WriteString(r.Continent)
	b.WriteByte('\n')
	return b.Bytes()
}

// Fast helpers for extracting fields from a CSV line []byte (no allocations)
func ExtractID(line []byte) int64 {
	// parse until first comma
	var i int64
	sign := 1
	for _, c := range line {
		if c == '-' {
			sign = -1
			continue
		}
		if c == ',' {
			break
		}
		i = i*10 + int64(c-'0')
	}
	return i * int64(sign)
}

// ExtractName returns slice indices (start,end) for the name field in the line
func ExtractNameSlice(line []byte) (start, end int) {
	// id,name,address,continent\n
	// find first comma
	n := len(line)
	i := 0
	for i < n && line[i] != ',' {
		i++
	}
	if i >= n {
		return 0, 0
	}
	start = i + 1
	j := start
	for j < n && line[j] != ',' {
		j++
	}
	end = j
	return
}

// ExtractContinentSlice returns start,end for continent field (last field, without trailing newline)
func ExtractContinentSlice(line []byte) (start, end int) {
	// we find last comma
	n := len(line)
	// remove trailing newline if present
	if n > 0 && line[n-1] == '\n' {
		n--
	}
	i := n - 1
	for i >= 0 && line[i] != ',' {
		i--
	}
	if i < 0 {
		return 0, 0
	}
	start = i + 1
	end = n
	return
}
