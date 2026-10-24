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
	//发送消息
	err = ch.Publish("", q.Name, false, false, amqp091.Publishing{
		Body: []byte("hello"),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("消息发送成功")

}
