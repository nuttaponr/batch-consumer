package adapter

import (
	"consumer/kafka"
	"consumer/something"
	"fmt"
	"strings"
)

func CallbackToSomething() kafka.CallbackFunc {
	return func(messages []*kafka.CallbackSessionMessage) error {
		printBuffer := []string{}
		for _, m := range messages {
			printBuffer = append(printBuffer, fmt.Sprintf("%v-%s", m.Message.Offset, string(m.Message.Value)))
		}
		str := strings.Join(printBuffer, ",")

		s := &something.Example{
			Message: str,
			Total:   len(printBuffer),
		}

		s.Do()
		// s.LongDo()

		return nil
	}
}
