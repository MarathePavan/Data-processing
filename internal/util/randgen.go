package util

import (
	"math/rand"
	"strings"
	"time"
)

type RandGen struct {
	r *rand.Rand
}

var continents = []string{
	"North America",
	"Asia",
	"South America",
	"Europe",
	"Africa",
	"Australia",
}

func NewRandGen(seed int64) *RandGen {
	src := rand.NewSource(seed)
	return &RandGen{r: rand.New(src)}
}

const letters = "abcdefghijklmnopqrstuvwxyz"
const lettersUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
const numbers = "0123456789"
const addressChars = letters + lettersUpper + numbers + " "

func (g *RandGen) Name() string {
	n := 10 + g.r.Intn(6)
	sb := strings.Builder{}
	sb.Grow(n)
	for i := 0; i < n; i++ {
		c := letters[g.r.Intn(len(letters))]
		sb.WriteByte(c)
	}
	if g.r.Intn(3) == 0 {
		s := sb.String()
		return strings.Title(s)
	}
	return sb.String()
}

func (g *RandGen) Address() string {
	n := 15 + g.r.Intn(6)
	sb := strings.Builder{}
	sb.Grow(n)
	for i := 0; i < n; i++ {
		sb.WriteByte(addressChars[g.r.Intn(len(addressChars))])
	}
	return strings.TrimSpace(sb.String())
}

func (g *RandGen) Continent() string {
	return continents[g.r.Intn(len(continents))]
}

func SeedFromTime() int64 { return time.Now().UnixNano() }
