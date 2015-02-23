package worker

import (
    "io"
    "errors"
    "sync"
    "here.com/scrooge/wet-docs/wet"
    "here.com/scrooge/wet-docs/workq"
    "here.com/scrooge/wet-docs/es"
)


type Worker struct {
    esClient *es.ElasticSearch
    queue workq.Queue
    done *sync.Mutex
    observer WorkerObserver
}

type WorkerObserver interface {
    FileStarted(filepath string)
    DocAdded()
    FileError()
    FileFinished()
    FlushOK()
    FlushFailed()
    AllDone()
}

func New(esHost, queueConf string, batchSize int, observer WorkerObserver) (w *Worker, err error) {
    q, err := workq.NewQueue(queueConf)
    if err == nil {
        var done sync.Mutex
        w = &Worker {
            esClient: es.NewElasticSearch(esHost, batchSize),
            queue: q,
            done: &done,
            observer: observer,
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
        if wet.Err == io.EOF {
            break
        } else if wet.Err != nil {
            return wet.Err
        } else {
            w.esClient.Add(wet.Entry)
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
            w.observer.FileError()
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
    w.queue.Close()
    w.done.Lock()
    defer w.done.Unlock()
}
