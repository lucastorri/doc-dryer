package workq


type localFilesQueue struct {
    channel <-chan Work
    stop chan bool
}

func (q *localFilesQueue) Channel() (<-chan Work, error) {
    return q.channel, nil
}

func (q *localFilesQueue) Close() {
    q.stop <- true
    close(q.stop)
    return
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


func newLocalFilesQueue(files []string) Queue {
    channel := make(chan Work)
    stop := make(chan bool)
    go func() {
        defer func() {
            close(channel)
        }()
        for _, f := range files {
            select {
                case channel <- &localFileWork { f }:
                case <-stop: break
            }

        }
    }()
    return &localFilesQueue { channel, stop }
}
