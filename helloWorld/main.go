package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	url := "amqp://user:password@127.0.0.1:5672"
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	go send(ctx, url)
	for i := 0; i < 5; i++ {
		go receive(ctx, url, i)
	}
	<-ctx.Done()
}

func send(ctx context.Context, url string) {
	conn, err := amqp.Dial(url)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer conn.Close()
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("%s\n", err)
		}
	}()
	ch, _ := conn.Channel()
	ch.Qos(1, 0, false)
	count := 0
	for {
		ticker := time.NewTicker(time.Second * 1)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q, _ := ch.QueueDeclare("hello", false, false, false, false, nil)
			ch.Publish("", q.Name, false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("hello world " + strconv.Itoa(count)),
			})
			count++
		}
	}
}

func receive(ctx context.Context, url string, i int) {
	conn, err := amqp.Dial(url)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer conn.Close()
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("%s\n", err)
		}
	}()
	ch, _ := conn.Channel()
	q, _ := ch.QueueDeclare("hello", false, false, false, false, nil)
	ch.Qos(1, 0, false)
	msg, _ := ch.Consume(q.Name, "", false, false, false, false, nil)
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-msg:
			log := strconv.Itoa(i) + " received a message: " + string(m.Body)
			if rand.Intn(10) == 0 { // 1/10的概率消费成功
				fmt.Println(log + " and consumed it")
				m.Ack(false)
			} else {
				fmt.Println(log + " but did not consume it")
				m.Reject(true)
			}
		}
	}
}
