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

	// 声明普通交换器
	err = ch.ExchangeDeclare(
		"normal_exchange", // name
		"direct",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 声明普通队列并设置死信交换器
	args := amqp091.Table{
		"x-dead-letter-exchange":    "dlx_exchange",
		"x-dead-letter-routing-key": "dlx_key",
	}
	_, err = ch.QueueDeclare(
		"normal_queue", // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		args,           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 绑定普通队列到普通交换器
	err = ch.QueueBind(
		"normal_queue",       // queue name
		"normal_routing_key", // routing key
		"normal_exchange",    // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// 消费普通队列中的消息
	msgs, err := ch.Consume(
		"normal_queue", // queue
		"",             // consumer
		false,          // auto-ack设为false，手动处理确认
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf(" [x] Received %s", d.Body)
			// 拒绝消息，不重新入队，触发死信机制
			d.Reject(false)
			log.Printf(" [x] Rejected message and sent to DLX")
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
