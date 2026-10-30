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

	err = ch.ExchangeDeclare(
		"yxjexchange",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("交换机声明失败")
	}

	q, err := ch.QueueDeclare(
		"yxjqueue",
		false,
		false,
		false,
		false,
		amqp091.Table{
			"x-max-priority": 10,
		},
	)
	if err != nil {
		log.Fatal("队列声明失败")
	}

	err = ch.QueueBind(
		q.Name,
		"",
		"yxjexchange",
		false,
		nil,
	)
	if err != nil {
		log.Fatal("队列绑定失败")
	}

	consumer, err := ch.Consume(
		"yxjqueue",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}
	for msg := range consumer {
		fmt.Println("消息内容：", string(msg.Body))
		msg.Ack(false)
	}

}
