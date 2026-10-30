package main

import (
	"fmt"
	"log"
	"rabbitmq/core"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
func consumerWithQos(consumerID string, count int) {
	ch := core.InitMQ()

	q, err := ch.QueueDeclare(
		"flow_control_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		count,
		0,
		false,
	)
	failOnError(err, "Failed to set QoS")
	// 消费消息
	msgs, err := ch.Consume(
		q.Name,
		consumerID,
		false, // 关闭自动确认，手动确认消息处理完成
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("消费者 %s 收到消息: %s", consumerID, d.Body)

			// 模拟处理耗时
			time.Sleep(500 * time.Millisecond)

			// 手动确认消息已处理
			d.Ack(false)
			log.Printf("消费者 %s 确认消息处理完成", consumerID)
		}
	}()

	log.Printf("消费者 %s (QoS控制) 启动，等待消息...", consumerID)
	<-forever
}

// 基于队列长度的流量控制
func producerWithQueueLengthControl() {
	ch := core.InitMQ()

	// 声明队列
	q, err := ch.QueueDeclare(
		"flow_control_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	maxQueueLength := 100 // 队列最大长度阈值

	for i := 1; i <= 1000; i++ {
		// 获取队列状态
		queueStatus, err := ch.QueueInspect(q.Name)
		failOnError(err, "Failed to inspect queue")

		// 如果队列长度超过阈值，等待
		if queueStatus.Messages >= maxQueueLength {
			log.Printf("队列长度 %d 超过阈值 %d，等待...", queueStatus.Messages, maxQueueLength)
			time.Sleep(2 * time.Second)
			i-- // 重试当前消息
			continue
		}

		body := fmt.Sprintf("这是第 %d 条消息", i)
		err = ch.Publish(
			"",
			q.Name,
			false,
			false,
			amqp091.Publishing{
				DeliveryMode: amqp091.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(body),
			})
		failOnError(err, "Failed to publish a message")

		log.Printf("已发送消息 %d (当前队列长度: %d)", i, queueStatus.Messages)
	}

	log.Println("所有消息发送完成")
}
func main() {
	go consumerWithQos("comsumer_1", 20)
	go consumerWithQos("consumer_2", 30)

	// 启动生产者（选择一种流量控制方式）
	go producerWithQueueLengthControl()

	// 保持程序运行
	time.Sleep(5 * time.Minute)
}
