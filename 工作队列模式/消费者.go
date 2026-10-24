package main

import (
	"fmt"
	"log"
	"rabbitmq/core"
)

func main() {
	//建立连接，创立通道
	ch := core.InitMQ()
	//申明队列
	q, err := ch.QueueDeclare("jiandan", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	//配置消费者的接收消息的能力
	err = ch.Qos(5, 0, false)

	consumer, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("接收消息")
	for msg := range consumer {
		fmt.Println("消息内容：", string(msg.Body))
		msg.Ack(false)
	}
}
