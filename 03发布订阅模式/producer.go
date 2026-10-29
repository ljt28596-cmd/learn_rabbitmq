package main

import (
	"fmt"
	"log"
	"rabbitmq/core"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	//建立连接，创建通道
	ch := core.InitMQ()

	//交换器名称
	exChangeName := "logs"

	err := ch.ExchangeDeclare(
		exChangeName,
		"fanout", //交换器类型
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		body := fmt.Sprintf("msg-%d", i)
		err = ch.Publish(
			exChangeName,
			"",
			false,
			false,
			amqp091.Publishing{
				Body: []byte(body),
			},
		)
		if err != nil {
			fmt.Println("发送失败：", err)
			return
		}
		fmt.Printf("发送消息：%s\n", body)
	}
}
