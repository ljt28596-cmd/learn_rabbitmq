package main

import (
	"fmt"
	"log"
	"rabbitmq/core"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	ch := core.InitMQ()

	// 4.消费消息
	consumer, err := ch.Consume("request_queue", "x1", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	for msg := range consumer {
		fmt.Println("消息内容", string(msg.Body), msg.CorrelationId)
		m := fmt.Sprintf("这是回来的消息 %s", msg.CorrelationId)
		err = ch.Publish("", msg.ReplyTo, false, false, amqp091.Publishing{
			Body:          []byte(m),
			CorrelationId: msg.CorrelationId,
		})
		if err != nil {
			fmt.Println("发送响应消息失败", err)
			continue
		}
		fmt.Println("发送响应消息", m)

	}
}
