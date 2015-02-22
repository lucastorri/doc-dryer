package main

import (
    "fmt"
    "io"
    "here.com/scrooge/wet-docs/wet"
    "here.com/scrooge/wet-docs/workq"
    "here.com/scrooge/wet-docs/es"
    "runtime"
)

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    //queueConf := "rabbit=amqp://guest:guest@localhost:5672/"
    queueConf := "local=/Users/lucastorri/Work/wet-stream/CC-MAIN-20141224185923-00096-ip-10-231-17-201.ec2.internal.warc.wet.gz"
    esHost := "http://127.0.01:9200/"

    esc := es.NewElasticSearch(esHost, 100)
    q, err := workq.NewQueue(queueConf)
    if err != nil {
        panic(err)
    }

    done := make(chan bool)
    go func() {
        fch := q.Channel()
        defer func() {
            done <- true
        }()
        for f := range fch {
            fmt.Println(f.Filepath())
            wr, err := wet.FromGZip(f.Filepath())
            if err == nil {
                wch := wr.Channel()
                for wet := range wch {
                    if wet.Err == io.EOF {
                        fmt.Println("!")
                    } else if wet.Err != nil {
                        panic(err)
                    } else {
                        esc.Add(wet.Entry)
                        fmt.Print(".")
                    }
                }
                f.Ack()
            } else {
                f.Nack()
            }
        }
        esc.Close()
    }()


    <-done
}
