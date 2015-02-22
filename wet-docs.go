package main

import (
    "fmt"
    "io"
    "here.com/scrooge/wet-docs/wet"
    "here.com/scrooge/wet-docs/workq"
    "runtime"
)

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    file := "/Users/lucastorri/Work/wet-stream/CC-MAIN-20141224185923-00096-ip-10-231-17-201.ec2.internal.warc.wet.gz"
    wr, err := wet.FromGZip(file)
    wch := wr.Channel()

    if err != nil {
        panic(err)
    }

    done := make(chan bool)

    go func() {
        defer func() {
            done <- true
        }()
        for wet := range wch {
            if wet.Err == io.EOF {
                fmt.Println("end")
            } else if wet.Err != nil {
                panic(err)
            } else {
                // fmt.Println(wet.Entry.Version)
            }
        }
    }()


    q, err := workq.NewQueue("local=" + file + "," + file)
    // q, err := workq.NewQueue("rabbit=amqp://guest:guest@localhost:5672/")
    if err != nil {
        panic(err)
    }

    fch := q.Channel()
    go func() {
        for f := range fch {
            fmt.Println(f.Filepath())
        }
    }()

    <-done
}
