package kafka

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

type slogWrap struct{}

func (s *slogWrap) Print(v ...interface{}) {
	slog.Debug(fmt.Sprintf("[sarama]: %v", v...))
}

func (s *slogWrap) Printf(format string, v ...interface{}) {
	info := fmt.Sprintf(format, v...)
	slog.Debug(fmt.Sprintf("[sarama]: %v", info))
}

func (s *slogWrap) Println(v ...interface{}) {
	slog.Debug(fmt.Sprintf("[sarama]: %v", v...))
}

type Producer struct {
	sarama.SyncProducer
}

func NewProducerKafka(c *Config) *Producer {
	slog.Info("Init kafka config")
	sarama.Logger = &slogWrap{}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = c.Retry
	config.Producer.Return.Successes = true
	info := fmt.Sprintf("[CONFIG] [PRODUCER] timeout:%v, brokers:%v", c.Timeout, c.Brokers)
	producerConfig, err := sarama.NewSyncProducer(c.Brokers, config)
	if err != nil {
		panic(err)
	}
	slog.Info("Kafka is connected: " + info)
	return &Producer{producerConfig}
}

func (p *Producer) Produce(xid, topic string, partition int32, message interface{}) error {
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal message for produce: %w", err)
	}

	part, o, err := p.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Value:     sarama.ByteEncoder(messageBytes),
		Headers:   []sarama.RecordHeader{{[]byte("X-Request-ID"), []byte(xid)}},
		Timestamp: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("unable to send message: %w", err)
	}

	slog.Info(fmt.Sprintf("produce message to partition %v and offset %v", part, o), slog.String("xid", xid))

	return nil
}
