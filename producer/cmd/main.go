package main

import (
	"log/slog"
	"os"
	"os/signal"
	"producer/kafka"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func init() {

	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
		// AddSource: true,
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
	slog.SetDefault(logger)
}

func main() {
	slog.Info("Hello")

	var c kafka.Config
	c.Brokers = []string{"localhost:9092"}
	c.Retry = 3
	c.Timeout = 30 * time.Second

	producer := kafka.NewProducerKafka(&c)
	go func() {
		for {
			producer.Produce(uuid.New().String(), "test", 0, time.Now().String())
			time.Sleep(time.Second * 1)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	slog.Info("shutdown server")

}
