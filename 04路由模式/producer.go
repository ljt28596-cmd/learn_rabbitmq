package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp091.Dial("amqp://admin:password@localhost:5672/")
	if err != nil {
		log.Fatalf("无法连接到 RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("无法打开通道: %v", err)
	}
	defer ch.Close()
	exChangeName := "logs_direct"

	err = ch.ExchangeDeclare(
		exChangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("声明交换器失败 %s", err)
	}

	for i := 0; i < 20; i++ {
		body := fmt.Sprintf("msg-%d", i)
		err = ch.Publish(
			exChangeName,
			"node001",
			false,
			false,
			amqp091.Publishing{
				Body: []byte(body),
			},
		)
		if err != nil {
			fmt.Printf("发送失败：%s\n", err)
			return
		}
		fmt.Printf("发送消息: %s\n", body)

	}
	for i := 0; i < 10; i++ {
		body := fmt.Sprintf("msg-%d", i)
		err = ch.Publish(
			exChangeName,
			"node002",
			false,
			false,
			amqp091.Publishing{
				Body: []byte(body),
			},
		)
		if err != nil {
			fmt.Printf("发送失败：%s\n", err)
			return
		}
		fmt.Printf("发送消息: %s\n", body)
	}
}
