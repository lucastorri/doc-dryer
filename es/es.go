package es

import (
    "bytes"
    "sync"
    "net/http"
    "encoding/json"
    "here.com/scrooge/wet-docs/wet"
)

var indexName = "common-crawl"
var indexType = "page"

type ElasticSearch struct {
    server string
    client *http.Client
    batchSize int
    itemsInBuffer int
    buffer []*wet.WETEntry
    ongoingRequests *sync.WaitGroup
    hadErrors bool
}

func NewElasticSearch(server string, batchSize int) *ElasticSearch {
    var ongoingRequests sync.WaitGroup
    var client http.Client
    es := &ElasticSearch { server, &client, batchSize, 0, nil, &ongoingRequests, false }
    es.newBuffer()
    return es
}

func (es *ElasticSearch) newBuffer() {
    es.itemsInBuffer = 0
    es.buffer = make([]*wet.WETEntry, es.batchSize)
}

func (es *ElasticSearch) submit() {
    docs := es.buffer[:es.itemsInBuffer]
    es.ongoingRequests.Add(1)

    go func() {
        var buffer bytes.Buffer
        for _, doc := range docs {
            serializeDocument(doc, &buffer)
        }

        url := es.server + "/_bulk"
        res, err := es.client.Post(url, "application/json", bytes.NewReader(buffer.Bytes()))
        if err == nil {
            res.Body.Close()
        } else {
            es.hadErrors = true
        }
        defer es.ongoingRequests.Done()
    }()
    es.newBuffer()
}

func serializeDocument(w *wet.WETEntry, buf *bytes.Buffer) {
    docId := w.Headers["WARC-Record-ID"]

    meta := make(map[string]string)
    meta["_index"] = indexName
    meta["_type"] = indexType
    meta["id"] = docId

    action := make(map[string]interface{})
    action["index"] = meta

    doc := make(map[string]string)
    doc["id"] = docId
    doc["url"] = w.Headers["WARC-Target-URI"]
    doc["content"] = string(w.Body)
    //TODO don't store _source

    encodedAction, _ := json.Marshal(action)
    encodedDoc, _ := json.Marshal(doc)

    buf.Write(encodedAction)
    buf.WriteString("\n")
    buf.Write(encodedDoc)
    buf.WriteString("\n")
}

func (es *ElasticSearch) isFull() bool {
    return es.itemsInBuffer == es.batchSize
}

func (es *ElasticSearch) isEmpty() bool {
    return es.itemsInBuffer == 0
}

func (es *ElasticSearch) enqueue(w *wet.WETEntry) {
    es.buffer[es.itemsInBuffer] = w
    es.itemsInBuffer++
}

func (es *ElasticSearch) Add(w *wet.WETEntry) {
    es.enqueue(w)
    if es.isFull() {
        es.submit()
    }
}

func (es *ElasticSearch) Flush() bool {
    if !es.isEmpty() {
        es.submit()
    }
    es.ongoingRequests.Wait()
    withErrors := es.hadErrors
    es.hadErrors = false
    es.newBuffer()
    return !withErrors
}
