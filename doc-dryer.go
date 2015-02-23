package main

import (
    "fmt"
    "here.com/scrooge/doc-dryer/worker"
    "runtime"
    "os"
    "os/signal"
    "syscall"
    "sync"
)

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    esHost := "http://127.0.01:9200/"
    queueConf := "rabbit=amqp://guest:guest@localhost:5672/"
    // queueConf := "local=/Users/lucastorri/Work/wet-stream/CC-MAIN-20141224185923-00096-ip-10-231-17-201.ec2.internal.warc.wet.gz"
    batchSize := 300

    workers, wg := createWorkers(1, esHost, queueConf, batchSize)

    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigs
        for _, w := range workers {
            w.Close()
        }
    }()

    for _, w := range workers {
        go func(w *worker.Worker) {
            w.Run()
        }(w)
        wg.Add(1)
    }

    wg.Wait()
}

func createWorkers(n int, esHost, queueConf string, batchSize int) ([]*worker.Worker, * sync.WaitGroup) {
    workers := make([]*worker.Worker, n)
    var wg sync.WaitGroup

    for i, _ := range workers {
        w, err := worker.New(esHost, queueConf, batchSize, &WorkerObserver { &wg })
        if err != nil {
            panic(err)
        }
        workers[i] = w
    }

    return workers, &wg
}

type WorkerObserver struct {
    wg *sync.WaitGroup
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
    o.wg.Done()
}
