package queue

import (
    "github.com/streadway/amqp"
    "net/http"
    "io"
    "io/ioutil"
    "time"
    "strings"
    "errors"
    "fmt"
)


var queueName = "work-queue"
var maxIdleTime time.Duration = 3 * time.Minute

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
        rmq.stop = make(chan bool, 1)
        go func() {
            defer func() {
                close(rmq.channel)
                close(rmq.stop)
                rmq.ch.Close()
                rmq.conn.Close()
            }()
            for {
                timer := time.NewTimer(maxIdleTime)
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

                        src := &downloadReader {
                            Reader: res.Body,
                            total: res.ContentLength,
                            stop: rmq.stop,
                        }

                        _, err = io.Copy(file, src)
                        if err != nil {
                            work.Nack()
                            continue
                        }

                        rmq.channel <- work
                    case <-timer.C:
                        return
                    case <-rmq.stop:
                        return
                }
                timer.Stop()
            }
        }()
    }
    return
}

func (rmq *rabbitMQ) Push(url string) error {
    msg := amqp.Publishing {
        DeliveryMode: amqp.Persistent,
        Timestamp:    time.Now(),
        ContentType:  "text/plain",
        Body:         []byte(strings.TrimSpace(url)),
    }
    return rmq.ch.Publish("", queueName, false, false, msg)
}

func (rmq *rabbitMQ) Close() {
    if rmq.stop != nil {
        rmq.stop <- true
    } else {
        rmq.ch.Close()
        rmq.conn.Close()
    }
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
            if err = ch.Qos(1, 0, false); err == nil {
                _, err = ch.QueueDeclare(queue, true, false, false, false, nil)
            }
        }
    }
    return
}

func newRabbitQueue(server string) (Queue, error) {
    r, err := newRabbit(server)
    if err == nil {
        r.init()
    }
    return r, err
}

func newRabbitPublisher(server string) (Publisher, error) {
    return newRabbit(server)
}

func newRabbit(server string) (r *rabbitMQ, err error) {
    if conn, ch, err := setup(server, queueName); err == nil {
        r = &rabbitMQ { conn, ch, nil, nil }
    }
    return
}

type downloadReader struct {
    io.Reader
    stop chan bool
    total int64
    downloaded int64
}

func (r *downloadReader) Read(p []byte) (int, error) {
    n, err := r.Reader.Read(p)
    r.downloaded += int64(n)

    select {
        case <-r.stop:
            err = errors.New("Downloaded aborted")
            r.stop <- true
        default:
    }

    if err == nil {
        fmt.Println("Downloaded", r.downloaded, " bytes of", r.total)
    }

    return n, err
}