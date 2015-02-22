package workq


type localFilesQueue struct {
    channel <-chan Work
}

func (q *localFilesQueue) Channel() (<-chan Work, error) {
    return q.channel, nil
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
    go func() {
        for _, f := range files {
            channel <- &localFileWork { f }
        }
        close(channel)
    }()
    return &localFilesQueue { channel }
}
