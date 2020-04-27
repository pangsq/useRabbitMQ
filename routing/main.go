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
	ch.ExchangeDeclare("logs_direct", "direct", true, false, false, false, nil)
	count := 0
	for {
		ticker := time.NewTicker(time.Second * 1)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i := strconv.Itoa(rand.Intn(5))
			ch.Publish("logs_direct", i, false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("hello world " + strconv.Itoa(count) + " to " + i),
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
	ch.ExchangeDeclare("logs_direct", "direct", true, false, false, false, nil)
	q, _ := ch.QueueDeclare("", false, false, true, false, nil)
	ch.QueueBind(q.Name, strconv.Itoa(i), "logs_direct", false, nil)
	msg, _ := ch.Consume(q.Name, "", true, false, false, false, nil)
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-msg:
			log := strconv.Itoa(i) + " received a message: " + string(m.Body)
			fmt.Println(log)
		}
	}
}
