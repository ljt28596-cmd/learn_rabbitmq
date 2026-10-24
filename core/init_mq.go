package core

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func InitMQ() *amqp091.Channel {
	//建立连接
	conn, err := amqp091.Dial("amqp://admin:password@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("连接成功")
	//创建通道
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	return ch
}
