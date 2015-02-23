package main

import (
    "fmt"
    "here.com/scrooge/doc-dryer/worker"
    "here.com/scrooge/doc-dryer/queue"
    "runtime"
    "os"
    "os/signal"
    "syscall"
    "sync"
    "flag"
    "io"
    "bufio"
)

var config struct {
    esHost string
    queueConf string
    batchSize int
    nWorkers int
    pathsFile string
}

func init() {
    flag.StringVar(&config.pathsFile, "publish", "", "file to be published")
    flag.StringVar(&config.queueConf, "queue", "rabbit=amqp://guest:guest@localhost:5672/", "rabbit=amqp://guest:guest@localhost:5672/ or file=file1,file2")
    flag.StringVar(&config.esHost, "es", "http://127.0.01:9200/", "elasticsearch endpoint")

    flag.IntVar(&config.batchSize, "batch", 300, "batch size for publishing to ElasticSearch")
    flag.IntVar(&config.nWorkers, "workers", 1, "number of workers to use")

    flag.Parse()
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    if config.pathsFile == "" {
        process()
    } else {
        publish()
    }
}

func process() {
    workers, wg := createWorkers()

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

func publish() {
    publisher, err := queue.NewPublisher(config.queueConf)
    if err != nil {
        panic(err)
    }
    defer publisher.Close()

    file, err := os.Open(config.pathsFile)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    reader := bufio.NewReader(file)
    for {
        url, err := reader.ReadString(byte('\n'))
        if err == io.EOF {
            return
        } else if err == nil {
            err = publisher.Push(url)
        }
        if err != nil {
            panic(err)
        }
    }
}

func createWorkers() ([]*worker.Worker, * sync.WaitGroup) {
    workers := make([]*worker.Worker, config.nWorkers)
    var wg sync.WaitGroup

    for i, _ := range workers {
        observer := &Observer { &wg }
        w, err := worker.New(config.esHost, config.queueConf, config.batchSize, observer, observer)
        if err != nil {
            panic(err)
        }
        workers[i] = w
    }

    return workers, &wg
}

type Observer struct {
    wg *sync.WaitGroup
}

func (o *Observer) FileStarted(filepath string) {
    fmt.Println(filepath)
}

func (o *Observer) DocAdded() {
    fmt.Print(".")
}

func (o *Observer) FileError(err error) {
    fmt.Print("x")
}

func (o *Observer) FileFinished() {
    fmt.Print("!")
}

func (o *Observer) FlushOK() {
    fmt.Println("+")
}

func (o *Observer) FlushFailed() {
    fmt.Println("-")
}

func (o *Observer) AllDone() {
    o.wg.Done()
}

func (o *Observer) WorkReceived(name string) {
    fmt.Println(name)
}

func (o *Observer) TransferProgress(downloaded, total int64) {
    fmt.Printf("%d/%d\n", downloaded, total)
}

func (o *Observer) TransferError() {
    fmt.Println("download error")
}

func (o *Observer) WorkReady() {
}
