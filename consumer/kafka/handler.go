package kafka

import (
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type CallbackFunc func(messages []*CallbackSessionMessage) error

type Option func(*ConsumerGroupHandler)

func RegisterCallback(callback CallbackFunc) Option {
	return func(cgh *ConsumerGroupHandler) {
		cgh.callback = callback
	}
}

type CallbackSessionMessage struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

func (csm *CallbackSessionMessage) MarkMessage() {
	csm.Session.MarkMessage(csm.Message, "")
}

type ConsumerGroupHandler struct {
	Ready          chan bool
	BatchCapacity  int
	TickerInterval int
	callback       CallbackFunc
	ticker         *time.Ticker
	buffer         []*CallbackSessionMessage
	mu             sync.RWMutex
}

func NewConsumerGroupHandler(opts ...Option) *ConsumerGroupHandler {
	h := &ConsumerGroupHandler{BatchCapacity: 10, TickerInterval: 10, Ready: make(chan bool)}
	h.ticker = time.NewTicker(time.Second * time.Duration(h.TickerInterval))
	for _, opt := range opts {
		opt(h)
	}
	h.buffer = make([]*CallbackSessionMessage, 0, h.BatchCapacity)
	if h.callback == nil {
		panic("plase register callback function")
	}

	return h
}

func (h *ConsumerGroupHandler) insertBuffer(msg *CallbackSessionMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.buffer = append(h.buffer, msg)
	if len(h.buffer) == h.BatchCapacity {
		h.flushBuffer()
	}
	h.ticker = time.NewTicker(time.Second * time.Duration(h.TickerInterval))
}

func (h *ConsumerGroupHandler) flushBuffer() {
	if len(h.buffer) > 0 {
		if err := h.callback(h.buffer); err == nil {
			clear(h.buffer)
			h.buffer = make([]*CallbackSessionMessage, 0, h.BatchCapacity)
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(h.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case messages, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			h.insertBuffer(&CallbackSessionMessage{session, messages})
		case <-h.ticker.C:
			slog.Info("ticker")
			h.mu.Lock()
			h.flushBuffer()
			h.mu.Unlock()
		}
	}
	return nil
}
