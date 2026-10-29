package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	//建立连接
	conn, err := amqp091.Dial("amqp://admin:password@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	fmt.Println("连接成功")
	//创建通道
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	//申明队列
	q, err := ch.QueueDeclare("jiandan", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	//消费消息
	consumer, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	for msg := range consumer {
		fmt.Println("消息内容：", string(msg.Body))
		msg.Ack(false)
	}
}
