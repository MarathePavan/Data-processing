package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
)

func brokersSlice(b string) []string {
	return strings.Split(b, ",")
}

func verifyOrderByID(brokers, topic string, sample int) (bool, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokersSlice(brokers),
		Topic:    topic,
		GroupID:  "mcp-verifier-id",
		MinBytes: 1,
		MaxBytes: 10 << 20,
	})
	defer r.Close()

	var prev int64 = -1
	for i := 0; i < sample; i++ {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			return false, err
		}
		s := string(m.Value)
		parts := strings.SplitN(strings.TrimSpace(s), ",", 2)
		id, _ := strconv.ParseInt(parts[0], 10, 64)
		if prev != -1 && id < prev {
			return false, nil
		}
		prev = id
	}
	return true, nil
}

func verifyOrderByName(brokers, topic string, sample int) (bool, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokersSlice(brokers),
		Topic:    topic,
		GroupID:  "mcp-verifier-name",
		MinBytes: 1,
		MaxBytes: 10 << 20,
	})
	defer r.Close()

	var prev string
	for i := 0; i < sample; i++ {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			return false, err
		}
		s := string(m.Value)
		parts := strings.SplitN(strings.TrimSpace(s), ",", 4)
		if len(parts) < 2 {
			continue
		}
		name := parts[1]
		if prev != "" && name < prev {
			return false, nil
		}
		prev = name
	}
	return true, nil
}

func verifyOrderByContinent(brokers, topic string, sample int) (bool, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokersSlice(brokers),
		Topic:    topic,
		GroupID:  "mcp-verifier-continent",
		MinBytes: 1,
		MaxBytes: 10 << 20,
	})
	defer r.Close()

	var prev string
	for i := 0; i < sample; i++ {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			return false, err
		}
		s := string(m.Value)
		parts := strings.SplitN(strings.TrimSpace(s), ",", 4)
		if len(parts) < 4 {
			continue
		}
		cont := parts[3]
		if prev != "" && cont < prev {
			return false, nil
		}
		prev = cont
	}
	return true, nil
}

func main() {
	var brokers = flag.String("brokers", "localhost:9092", "kafka brokers")
	var sample = flag.Int("sample", 1000, "sample size to verify ordering")
	flag.Parse()

	fmt.Println("Verifier: sampling topics id, name, continent")

	ok, err := verifyOrderByID(*brokers, "id", *sample)
	if err != nil {
		fmt.Printf("verify id error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("id sorted? %v\n", ok)

	ok, err = verifyOrderByName(*brokers, "name", *sample)
	if err != nil {
		fmt.Printf("verify name error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("name sorted? %v\n", ok)

	ok, err = verifyOrderByContinent(*brokers, "continent", *sample)
	if err != nil {
		fmt.Printf("verify continent error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("continent sorted? %v\n", ok)

	fmt.Println("Verifier done.")
}
