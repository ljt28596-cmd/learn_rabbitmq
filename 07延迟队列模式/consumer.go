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

	// 声明死信交换器
	dlxExchange := "dlx_exchange"
	err = ch.ExchangeDeclare(
		dlxExchange, // name
		"direct",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 声明死信队列
	dlQueueName := "dl_queue"
	_, err = ch.QueueDeclare(
		dlQueueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 绑定死信队列到死信交换器
	err = ch.QueueBind(
		dlQueueName, // queue name
		"dl_key",    // routing key
		dlxExchange, // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// 声明延迟队列（设置x-dead-letter-exchange和x-message-ttl参数）
	delayQueueName := "delay_queue"
	_, err = ch.QueueDeclare(
		delayQueueName, // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		amqp091.Table{
			"x-dead-letter-exchange":    dlxExchange,
			"x-dead-letter-routing-key": "dl_key",
			"x-message-ttl":             5000, // 5秒后消息过期
		}, // arguments
	)
	failOnError(err, "Failed to declare a queue")

	var forever chan struct{}
	msgs, _ := ch.Consume(
		"dl_queue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	go func() {
		for d := range msgs {
			log.Printf(" [x] Received %s", d.Body)
			log.Printf("当前时间: %s", time.Now().Format("2006-01-02 15:04:05"))
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
