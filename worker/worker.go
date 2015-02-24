package worker

import (
    "io"
    "errors"
    "sync"
    "github.com/lucastorri/doc-dryer/wet"
    "github.com/lucastorri/doc-dryer/queue"
    "github.com/lucastorri/doc-dryer/es"
)


type Worker struct {
    esClient *es.ElasticSearch
    queue queue.Queue
    done *sync.Mutex
    observer WorkerObserver
    stop chan bool
}

type WorkerObserver interface {
    FileStarted(filepath string)
    DocAdded()
    FileError(err error)
    FileFinished()
    FlushOK()
    FlushFailed()
    AllDone()
}

func New(esHost, queueConf string, batchSize int, wo WorkerObserver, qo queue.QueueObserver) (w *Worker, err error) {
    q, err := queue.NewQueue(queueConf, qo)
    if err == nil {
        var done sync.Mutex
        w = &Worker {
            esClient: es.NewElasticSearch(esHost, batchSize),
            queue: q,
            done: &done,
            observer: wo,
            stop: make(chan bool, 1),
        }
    }
    return
}

func (w *Worker) processFile(filepath string) error {
    wr, err := wet.FromGZip(filepath)
    if err != nil {
        return err
    }
    wch := wr.Channel()
    for wet := range wch {
        select {
            case <-w.stop: return errors.New("Worker stopped in mid work")
            default:
        }
        if wet.Err == io.EOF {
            break
        } else if wet.Err != nil {
            return wet.Err
        } else {
            if !w.esClient.Add(wet.Entry) {
                return errors.New("Errors while submitting files to index")
            }
            w.observer.DocAdded()
        }
    }
    if !w.esClient.Flush() {
        return errors.New("Errors while submitting files to index")
    }
    return nil
}

func (w *Worker) Run() {
    w.done.Lock()
    defer w.done.Unlock()
    fch := w.queue.Channel()
    for f := range fch {
        filepath := f.Filepath()
        w.observer.FileStarted(filepath)
        err := w.processFile(filepath)
        if err == nil {
            w.observer.FileFinished()
            err = f.Ack()
        } else {
            w.observer.FileError(err)
            err = f.Nack()
        }
        if err == nil {
            w.observer.FlushOK()
        } else {
            w.observer.FlushFailed()
        }
    }
    w.observer.AllDone()
}

func (w *Worker) Close() {
    w.stop <- true
    close(w.stop)
    w.queue.Close()
    w.done.Lock()
    defer w.done.Unlock()
}
