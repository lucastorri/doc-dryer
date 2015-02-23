package queue

import (
    "github.com/streadway/amqp"
    "net/http"
    "io"
    "io/ioutil"
    "time"
)


var queueName = "work-queue"
var maxIdleTime time.Duration = 3 * time.Minute

//TODO close itself after N minute idle
type rabbitMQ struct {
    conn *amqp.Connection
    ch *amqp.Channel
    channel chan Work
    stop chan bool
}

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
                timer := time.AfterFunc(maxIdleTime, func() {})
                select {
                    case delivery := <- deliveries:
                        work := &rabbitMQWork { rmq, delivery, "" }
                        var err error

                        file, err := ioutil.TempFile("", "doc-dryer-wet-part-")
                        if err != nil {
                            work.Nack()
                            continue
                        }
                        defer file.Close()

                        work.filepath = file.Name()
                        res, err := http.Get(string(delivery.Body))
                        if err != nil {
                            work.Nack()
                            continue
                        }
                        defer res.Body.Close()

                        _, err = io.Copy(file, res.Body)
                        if err != nil {
                            work.Nack()
                            continue
                        }

                        rmq.channel <- work
                    case <-timer.C:
                        break
                    case <-rmq.stop:
                        break
                }
                timer.Stop()
            }
        }()
    }
    return
}

func (rmq *rabbitMQ) Close() {
    rmq.stop <- true
}


type rabbitMQWork struct {
    rmq *rabbitMQ
    delivery amqp.Delivery
    filepath string
}

func (w *rabbitMQWork) Filepath() string {
    return w.filepath
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
