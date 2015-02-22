package workq

import (
    "github.com/streadway/amqp"
)


type rabbitMQ struct {
    conn *amqp.Connection
    ch *amqp.Channel
}

type rabbitMQWork struct {
    rmq *rabbitMQ
    delivery amqp.Delivery
}

var queueName = "work-queue"

func (rmq *rabbitMQ) Channel() (channel <-chan Work, err error) {
    deliveries, err := rmq.ch.Consume(queueName, "", false, false, false, false, nil)
    if err == nil {
        ch := make(chan Work)
        channel = ch
        go func() {
            for delivery := range deliveries {
                work := &rabbitMQWork { rmq, delivery }
                ch <- work
            }
        }()
    }
    return
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
        q = &rabbitMQ { conn, ch }
    }
    return
}
