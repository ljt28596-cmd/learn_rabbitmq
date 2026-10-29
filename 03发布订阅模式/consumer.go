package main

import (
	"fmt"
	"log"
	"rabbitmq/core"
)

func main() {
	ch := core.InitMQ()
	exChangeName := "logs"

	err := ch.ExchangeDeclare(
		exChangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	//创建临时队列
	q, err := ch.QueueDeclare(
		"",
		false, //durable:服务重启后是否依然存活
		true,  //auto-deleted:没有消费者后是否自动删除
		true,  //exclusive-queue:是否仅自己可用
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("声明临时队列失败 %s", err)
	}
	err = ch.QueueBind(
		q.Name,
		"",
		exChangeName,
		false,
		nil,
	)
	consumer, err := ch.Consume(
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
	for d := range consumer {
		log.Println("[x] 收到消息:", string(d.Body))
	}
}
