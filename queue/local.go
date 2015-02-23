package queue

import (
    "errors"
)


type localFilesQueue struct {
    channel <-chan Work
    stop chan bool
}

func (q *localFilesQueue) Channel() (<-chan Work) {
    return q.channel
}

func (q *localFilesQueue) Close() {
    q.stop <- true
    close(q.stop)
}


type localFileWork struct {
    filepath string
}

func (w *localFileWork) Filepath() string {
    return w.filepath
}

func (w *localFileWork) Ack() error {
    return nil
}

func (w *localFileWork) Nack() error {
    return nil
}


func newLocalFilesQueue(files []string, observer QueueObserver) Queue {
    channel := make(chan Work)
    stop := make(chan bool)
    go func() {
        defer func() {
            close(channel)
        }()
        for _, f := range files {
            observer.WorkReceived(f)
            observer.WorkReady()
            select {
                case channel <- &localFileWork { f }:
                case <-stop: break
            }
        }
    }()
    return &localFilesQueue { channel, stop }
}

func newLocalFilePublisher() (Publisher, error) {
    return nil, errors.New("Can not publish to local queue")
}
