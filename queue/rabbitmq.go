package queue

import (
    "github.com/streadway/amqp"
)


type rabbitMQ struct {
    conn *amqp.Connection
    ch *amqp.Channel
    channel chan Work
    stop chan bool
}

type rabbitMQWork struct {
    rmq *rabbitMQ
    delivery amqp.Delivery
}

var queueName = "work-queue"

func (rmq *rabbitMQ) Channel() (channel <-chan Work) {
    return rmq.channel
}

func (rmq *rabbitMQ) init() (err error) {
    if deliveries, err := rmq.ch.Consume(queueName, "", false, false, false, false, nil); err == nil {
        rmq.channel = make(chan Work)
        rmq.stop = make(chan bool)
        go func() {
            defer func() {
                close(rmq.channel)
                close(rmq.stop)
                rmq.ch.Close()
                rmq.conn.Close()
            }()
            for {
                select {
                    case delivery := <- deliveries:
                        //TODO to finish, have to download the file, put in a tmp dir, and pass the filepath on
                        //http://stackoverflow.com/questions/11692860/how-can-i-efficiently-download-a-large-file-using-go
                        work := &rabbitMQWork { rmq, delivery }
                        rmq.channel <- work
                    case <-rmq.stop:
                        break
                }
            }
        }()
    }
    return
}

func (rmq *rabbitMQ) Close() {
    rmq.stop <- true
}


func (w *rabbitMQWork) Filepath() string {
    return string(w.delivery.Body)
}

func (w *rabbitMQWork) Ack() error {
    return w.delivery.Ack(false)
}

func (w *rabbitMQWork) Nack() error {
    return w.delivery.Nack(false, true)
}

func setup(url, queue string) (conn *amqp.Connection, ch *amqp.Channel, err error) {
    if conn, err = amqp.Dial(url); err == nil {
        if ch, err = conn.Channel(); err == nil {
            // if err = ch.Qos(1, 0, false); err == nil {
                _, err = ch.QueueDeclare(queue, true, false, false, false, nil)
            // }
        }
    }
    return
}

func newRabbitQueue(server string) (q Queue, err error) {
    if conn, ch, err := setup(server, queueName); err == nil {
        nq := &rabbitMQ { conn, ch, nil, nil }
        err = nq.init()
        q = nq
    }
    return
}
