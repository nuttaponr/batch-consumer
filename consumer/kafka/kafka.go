package kafka

import (
	"fmt"
	"log/slog"

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

type Consumer struct {
	sarama.ConsumerGroup
}

func NewConsumerConfig(c *Config) sarama.ConsumerGroup {
	slog.Info("Init kafka consumer config")
	sarama.Logger = &slogWrap{}
	config := sarama.NewConfig()
	config.Version = sarama.DefaultVersion
	switch c.GroupStrategy {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategySticky()
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	default:
		slog.Error("unrecognized consumer group rebalance strategy")
	}

	if c.IsOffsetOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	consumerClient, err := sarama.NewConsumerGroup(c.Brokers, c.GroupID, config)
	if err != nil {
		panic(err.Error())
	}

	info := fmt.Sprintf("[CONFIG] [CONSUMER] brokers:%v, group-strategy:%s, group-id:%s", c.Brokers, c.GroupStrategy, c.GroupID)
	slog.Info("consumer client is ok: " + info)
	return consumerClient
}
