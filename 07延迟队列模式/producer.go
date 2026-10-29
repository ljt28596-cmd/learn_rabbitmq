package main

import (
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func main() {
	conn, err := amqp091.Dial("amqp://admin:password@localhost/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 发送消息到延迟队列
	body := "Hello, delayed message!"
	err = ch.Publish(
		"",            // exchange
		"delay_queue", // routing key
		false,         // mandatory
		false,         // immediate
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
	log.Println("消息已发送到延迟队列，5秒后将进入死信队列")

	time.Sleep(2 * time.Second) // 等待消息发送完成
}
