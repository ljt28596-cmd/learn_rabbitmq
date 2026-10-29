package main

import (
	"fmt"
	"log"
	"rabbitmq/core"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	//建立连接，创立通道
	ch := core.InitMQ()
	q, err := ch.QueueDeclare("jiandan", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		msg := fmt.Sprintf("消息%d", i)

		ch.Publish("", q.Name, false, false, amqp091.Publishing{
			Body: []byte(msg),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("消息发送成功")
	}

}
