package kafka

import "time"

type Config struct {
	Brokers []string
	Retry   int
	Timeout time.Duration
}
