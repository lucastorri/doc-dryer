package main

import (
    "fmt"
    "here.com/scrooge/wet-docs/worker"
    "runtime"
)

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    esHost := "http://127.0.01:9200/"
    //queueConf := "rabbit=amqp://guest:guest@localhost:5672/"
    queueConf := "local=/Users/lucastorri/Work/wet-stream/CC-MAIN-20141224185923-00096-ip-10-231-17-201.ec2.internal.warc.wet.gz"
    batchSize := 300

    done := make(chan bool)
    w, err := worker.New(esHost, queueConf, batchSize, &WorkerObserver{ done })
    if err != nil {
        panic(err)
    }

    go func() {
        w.Run()
    }()

    <-done
}

type WorkerObserver struct {
    done chan<- bool
}

func (o *WorkerObserver) FileStarted(filepath string) {
    fmt.Println(filepath)
}

func (o *WorkerObserver) DocAdded() {
    fmt.Print(".")
}

func (o *WorkerObserver) FileError(err error) {
    fmt.Print("x")
}

func (o *WorkerObserver) FileFinished() {
    fmt.Print("!")
}

func (o *WorkerObserver) FlushOK() {
    fmt.Println("+")
}

func (o *WorkerObserver) FlushFailed() {
    fmt.Println("-")
}

func (o *WorkerObserver) AllDone() {
    o.done <- true
}
