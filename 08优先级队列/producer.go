package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp091.Dial("amqp://admin:password@localhost/")
	if err != nil {
		log.Fatal("连接失败")
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("建立通道失败")
	}
	type msgs struct {
		body     string
		priority uint8
	}

	messages := []msgs{
		{"这是低优先级消息 (1)", 1},
		{"这是中优先级消息 (5)", 5},
		{"这是高优先级消息 (10)", 10},
		{"这是另一个低优先级消息 (2)", 2},
		{"这是另一个高优先级消息 (9)", 9},
	}

	for _, msg := range messages {
		err := ch.Publish(
			"yxjexchange",
			"",
			false,
			false,
			amqp091.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.body),
				Priority:    msg.priority,
			},
		)
		if err != nil {
			fmt.Println("发送失败")
		}
		log.Printf("已发送: %s 优先级: %d", msg.body, msg.priority)
	}
}
