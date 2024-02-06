package kafka

type Config struct {
	Brokers        []string
	GroupStrategy  string
	IsOffsetOldest bool
	GroupID        string
}
