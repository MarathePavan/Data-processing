package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	kafkawrap "mcp-data-sorter/internal/kafka"
	"mcp-data-sorter/internal/schema"
	"mcp-data-sorter/internal/util"
)

func main() {
	var (
		brokers   = flag.String("brokers", "localhost:9092", "Kafka broker (host:port)")
		topic     = flag.String("topic", "source", "Kafka topic to produce to")
		count     = flag.Int64("count", 1000000, "Number of records to generate")
		batchSize = flag.Int("batch", 2000, "Producer batch size")
		seed      = flag.Int64("seed", time.Now().UnixNano(), "Random seed")
	)
	flag.Parse()

	fmt.Printf("Generator starting: brokers=%s topic=%s count=%d batch=%d seed=%d\n",
		*brokers, *topic, *count, *batchSize, *seed)

	prod, err := kafkawrap.NewProducer(kafkawrap.ProducerConfig{
		Brokers:    []string{*brokers},
		Topic:      *topic,
		BatchSize:  *batchSize,
		BatchBytes: 4 << 20,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create producer: %v\n", err)
		os.Exit(1)
	}
	defer prod.Close()

	rg := util.NewRandGen(*seed)
	ctx := context.Background()
	start := time.Now()
	var produced int64

	batch := make([]kafkawrap.Message, 0, *batchSize)
	for i := int64(0); i < *count; i++ {
		rec := schema.Record{
			ID:        int32(i + 1),
			Name:      rg.Name(),
			Address:   rg.Address(),
			Continent: rg.Continent(),
		}
		line := rec.ToCSV()
		msg := kafkawrap.Message{
			Key:   []byte(fmt.Sprintf("%d", rec.ID)),
			Value: line,
		}
		batch = append(batch, msg)

		if len(batch) >= *batchSize {
			if err := prod.WriteMessages(ctx, batch...); err != nil {
				fmt.Fprintf(os.Stderr, "producer write error: %v\n", err)
				os.Exit(1)
			}
			produced += int64(len(batch))
			batch = batch[:0]
			if produced%500000 == 0 {
				fmt.Printf("Produced %d records so far\n", produced)
			}
		}
	}
	if len(batch) > 0 {
		if err := prod.WriteMessages(ctx, batch...); err != nil {
			fmt.Fprintf(os.Stderr, "producer final write error: %v\n", err)
			os.Exit(1)
		}
		produced += int64(len(batch))
	}
	elapsed := time.Since(start)
	fmt.Printf("Generator finished. Produced=%d elapsed=%s rate=%.0f rec/s\n",
		produced, elapsed.String(), float64(produced)/elapsed.Seconds())
}
