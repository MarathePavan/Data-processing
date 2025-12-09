package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	kafkawrap "mcp-data-sorter/internal/kafka"
	sortpkg "mcp-data-sorter/internal/sort"
	"mcp-data-sorter/internal/util"

	"github.com/segmentio/kafka-go"
)

func main() {
	var (
		brokers = flag.String("brokers", "localhost:9092", "kafka brokers")
		source  = flag.String("source", "source", "source topic")
		count   = flag.Int64("count", 1000000, "number of records to consume")
		memBuf  = flag.Int("membuf", 150000, "approx records per in-memory buffer before flushing a run")
		runDir  = flag.String("rundir", "/data/runs", "directory for temporary runs")
	)
	flag.Parse()

	fmt.Printf("Sorter starting: brokers=%s source=%s count=%d membuf=%d runDir=%s\n",
		*brokers, *source, *count, *memBuf, *runDir)

	if err := os.MkdirAll(*runDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to make rundir: %v\n", err)
		os.Exit(1)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{*brokers},
		Topic:    *source,
		GroupID:  "mcp-sorter-group-1",
		MinBytes: 1,
		MaxBytes: 10 << 20,
	})
	defer r.Close()

	lines := make(chan string, 10000)

	rg := sortpkg.NewRunGenerator(*runDir, *memBuf)

	go func() {
		var read int64
		for read < *count {
			m, err := r.FetchMessage(context.Background())
			if err != nil {
				fmt.Fprintf(os.Stderr, "error fetching message: %v\n", err)
				break
			}
			s := string(m.Value)
			if !strings.HasSuffix(s, "\n") {
				s = s + "\n"
			}
			lines <- s
			read++
			if err := r.CommitMessages(context.Background(), m); err != nil {
				fmt.Fprintf(os.Stderr, "commit offset error: %v\n", err)
			}
		}
		close(lines)
	}()

	t0 := util.NewTimer("run-generation")
	idRuns, nameRuns, contRuns, err := rg.GenerateRuns(lines)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run generation failed: %v\n", err)
		os.Exit(1)
	}
	t0.Print()

	mergeAndProduce := func(key string, runs []string, outTopic string) error {
		fmt.Printf("Merging %d run files for %s -> topic %s\n", len(runs), key, outTopic)
		p, err := kafkawrap.NewProducer(kafkawrap.ProducerConfig{
			Brokers:    []string{*brokers},
			Topic:      outTopic,
			BatchSize:  2000,
			BatchBytes: 4 << 20,
		})
		if err != nil {
			return err
		}
		defer p.Close()

		var wrote int64
		lineHandler := func(line string) error {
			msg := kafkawrap.Message{
				Key:   nil,
				Value: []byte(line),
			}
			ctx := context.Background()
			if err := p.WriteMessages(ctx, msg); err != nil {
				return err
			}
			wrote++
			return nil
		}
		start := time.Now()
		if err := sortpkg.MergeRuns(runs, lineHandler, key); err != nil {
			return err
		}
		fmt.Printf("Merged->%s wrote=%d elapsed=%s\n", outTopic, wrote, time.Since(start).String())
		return nil
	}

	// sequential merges to conserve memory
	if err := mergeAndProduce("id", idRuns, "id"); err != nil {
		fmt.Fprintf(os.Stderr, "merge id failed: %v\n", err)
		os.Exit(1)
	}
	if err := mergeAndProduce("name", nameRuns, "name"); err != nil {
		fmt.Fprintf(os.Stderr, "merge name failed: %v\n", err)
		os.Exit(1)
	}
	if err := mergeAndProduce("continent", contRuns, "continent"); err != nil {
		fmt.Fprintf(os.Stderr, "merge continent failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Sorter finished successfully.")

	fmt.Println("Cleaning up run files...")
	filepath.Walk(*runDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(path, ".tmp") {
			_ = os.Remove(path)
		}
		return nil
	})
}
