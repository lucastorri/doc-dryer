package queue

import (
    "strings"
    "errors"
)


type Work interface {
    Filepath() string
    Ack() error
    Nack() error
}

type Queue interface {
    Channel() (<-chan Work)
    Close()
}

type Publisher interface {
    Push(url string) error
    Close()
}


func NewQueue(config string) (q Queue, err error) {

    configParts := strings.Split(config, "=")
    switch configParts[0] {
        case "rabbit":
            q, err = newRabbitQueue(configParts[1])
        case "local":
            q = newLocalFilesQueue(strings.Split(configParts[1], ","))
        default:
            panic(errors.New("Invalid queue configuration: " + config))
    }

    return
}

func NewPublisher(config string) (p Publisher, err error) {

    configParts := strings.Split(config, "=")
    switch configParts[0] {
        case "rabbit":
            p, err = newRabbitPublisher(configParts[1])
        case "local":
            p, err = newLocalFilePublisher()
        default:
            panic(errors.New("Invalid queue configuration: " + config))
    }

    return
}
