package main

import (
	"consumer/adapter"
	"consumer/kafka"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
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

	var c kafka.Config
	c.Brokers = []string{"localhost:9092"}
	c.GroupID = "hello"
	c.GroupStrategy = "sticky"
	c.IsOffsetOldest = true

	consumerClient := kafka.NewConsumerConfig(&c)
	handler := kafka.NewConsumerGroupHandler(kafka.RegisterCallback(adapter.CallbackToSomething()))
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerClient.Consume(ctx, []string{"test"}, handler); err != nil {
				slog.Error("error from consumer", slog.String("error", err.Error()))
			}
			if ctx.Err() != nil {
				return
			}
			handler.Ready = make(chan bool)
		}
	}()
	<-handler.Ready
	slog.Info("⇨ consumer ready")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	cancel()

	slog.Info("shutdown server: ")

}
