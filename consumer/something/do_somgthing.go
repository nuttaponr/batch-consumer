package something

import (
	"fmt"
	"time"
)

type Example struct {
	Message string
	Total   int
}

func (m *Example) Do() {
	fmt.Printf("Batch lenght: %v, Batch messages: %s\n", m.Total, m.Message)
}

func (m *Example) LongDo() {
	fmt.Println("start long do")
	time.Sleep(15 * time.Second)
	fmt.Println("end long do")
}
