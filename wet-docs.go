package main

import (
    "fmt"
    "io"
    "here.com/scrooge/wet-docs/wet"
)

func main() {
    file := "/Users/lucastorri/Work/wet-stream/CC-MAIN-20141224185923-00096-ip-10-231-17-201.ec2.internal.warc.wet.gz"
    wr, err := wet.FromGZip(file)
    for i := 0; err == nil; i++ {
        fmt.Println(i)
        _, err = wr.Next()
    }
    if err == io.EOF {
        fmt.Println("end")
    } else if err != nil {
        panic(err)
    }

}
