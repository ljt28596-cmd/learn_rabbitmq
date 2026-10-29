package main

import (
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp091.Dial("amqp://admin:password@localhost/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 声明死信交换器
	err = ch.ExchangeDeclare(
		"dlx_exchange", // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 声明死信队列
	_, err = ch.QueueDeclare(
		"dlx_queue", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 绑定死信队列到死信交换器
	err = ch.QueueBind(
		"dlx_queue",    // queue name
		"dlx_key",      // routing key
		"dlx_exchange", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// 消费死信队列中的消息
	msgs, err := ch.Consume(
		"dlx_queue", // queue
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf(" [x] DLX Received %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for DLX messages. To exit press CTRL+C")
	<-forever
}
