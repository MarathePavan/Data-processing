package kafka

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
)

type ProducerConfig struct {
	Brokers    []string
	Topic      string
	BatchSize  int
	BatchBytes int
}

type Message = kafka.Message

type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("no brokers provided")
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		Balancer:               &kafka.LeastBytes{},
		BatchSize:              cfg.BatchSize,
		BatchBytes:             int64(cfg.BatchBytes),
		AllowAutoTopicCreation: true,
	}
	return &Producer{
		writer: w,
		topic:  cfg.Topic,
	}, nil
}

func (p *Producer) WriteMessages(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}
	return p.writer.WriteMessages(ctx, msgs...)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
