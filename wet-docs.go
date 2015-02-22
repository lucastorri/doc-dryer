package main

import (
    "fmt"
    "io"
    "errors"
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

    esc := es.NewElasticSearch(esHost, 300)
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
                        break
                    } else if wet.Err != nil {
                        err = wet.Err
                        break
                    } else {
                        esc.Add(wet.Entry)
                        fmt.Print(".")
                    }
                }
                if !esc.Flush() {
                    err = errors.New("Errors while submitting files to index")
                }
            }
            if err == nil {
                err = f.Ack()
                fmt.Print("!")
            } else {
                err = f.Nack()
                fmt.Print("x")
            }
            if err == nil {
                fmt.Println("+")
            } else {
                fmt.Println("-")
            }
        }
    }()


    <-done
}
