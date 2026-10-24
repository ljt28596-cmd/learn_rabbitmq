package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	url       = "amqp://admin:password@localhost:5672/"
	queueName = "qos_demo"
	total     = 20
)

var (
	start   = time.Now()
	done    int64
	doneOnce sync.Once
	allDone = make(chan struct{})
	closers []func()
	closerMu sync.Mutex
)

func ts() string { return fmt.Sprintf("[%5dms]", time.Since(start).Milliseconds()) }

func main() {
	mode := ""
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	// 清空队列，保证每次实验条件一致
	c := connect()
	ch := channel(c)
	if _, err := ch.QueueDeclare(queueName, false, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	if _, err := ch.QueuePurge(queueName, false); err != nil {
		log.Fatal(err)
	}
	ch.Close()
	c.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go consumer("A", 4, 50*time.Millisecond, &wg)
	bDelay := 50 * time.Millisecond
	if mode == "slow" {
		bDelay = 300 * time.Millisecond
	}
	go consumer("B", 5, bDelay, &wg)

	time.Sleep(500 * time.Millisecond) // 等两个消费者都订阅上

	p := connect()
	pch := channel(p)
	for i := 1; i <= total; i++ {
		if err := pch.Publish("", queueName, false, false,
			amqp091.Publishing{Body: []byte(fmt.Sprintf("消息%d", i))}); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(ts(), ">>> 生产者已一口气发出全部 20 条消息")
	pch.Close()
	p.Close()

	go func() {
		<-allDone
		closerMu.Lock()
		for _, f := range closers {
			f()
		}
		closerMu.Unlock()
	}()

	wg.Wait()
}

func consumer(name string, prefetch int, delay time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	conn := connect()
	ch := channel(conn)
	if err := ch.Qos(prefetch, 0, false); err != nil {
		log.Fatal(err)
	}
	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	closerMu.Lock()
	closers = append(closers, func() { conn.Close() })
	closerMu.Unlock()
	fmt.Println(ts(), "消费者"+name+"已订阅, Qos prefetch =", prefetch)

	count := 0
	for m := range msgs {
		fmt.Println(ts(), name+"收到   ", string(m.Body))
		time.Sleep(delay) // 模拟处理耗时
		m.Ack(false)
		count++
		fmt.Println(ts(), name+"处理完并Ack", string(m.Body))
		if atomic.AddInt64(&done, 1) == total {
			doneOnce.Do(func() { close(allDone) })
		}
	}
	fmt.Println(ts(), "<<< 消费者"+name+"退出, 共处理", count, "条")
}

func connect() *amqp091.Connection {
	conn, err := amqp091.Dial(url)
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func channel(c *amqp091.Connection) *amqp091.Channel {
	ch, err := c.Channel()
	if err != nil {
		log.Fatal(err)
	}
	return ch
}
