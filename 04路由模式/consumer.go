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

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("声明临时队列失败 %s", err)
	}

	ch.QueueBind(
		q.Name,
		"node002",
		exChangeName,
		false,
		nil,
	)
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("创建消费者失败 %s", err)
	}
	fmt.Println("准备接收消息")
	for d := range msgs {
		fmt.Printf("收到消息：%s\n", d.Body)
	}
}
