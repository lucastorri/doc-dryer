package es

import (
    "bytes"
    "net/http"
    "encoding/json"
    "here.com/scrooge/wet-docs/wet"
)

var indexName = "common-crawl"
var indexType = "page"

type ElasticSearch struct {
    client *http.Client
    server string
    batchSize int
    total int
    buffer []*wet.WETEntry
}

func NewElasticSearch(server string, batchSize int) *ElasticSearch {
    es := &ElasticSearch { &http.Client{}, server, batchSize, 0, nil }
    es.newBuffer()
    return es
}

func (es *ElasticSearch) newBuffer() {
    es.total = 0
    es.buffer = make([]*wet.WETEntry, es.batchSize)
}

func (es *ElasticSearch) submit() {
    docs := es.buffer
    go func() {
        var buffer bytes.Buffer
        for _, doc := range docs {
            serializeDocument(doc, &buffer)
        }

        url := es.server + "/_bulk"
        res, _ := http.Post(url, "application/json", bytes.NewReader(buffer.Bytes()))
        defer res.Body.Close()

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
    return es.total == es.batchSize
}

func (es *ElasticSearch) isEmpty() bool {
    return es.total == 0
}

func (es *ElasticSearch) enqueue(w *wet.WETEntry) {
    es.buffer[es.total] = w
    es.total++
}

func (es *ElasticSearch) Add(w *wet.WETEntry) {
    es.enqueue(w)
    if es.isFull() {
        es.submit()
    }
}

func (es *ElasticSearch) Close() {
    if !es.isEmpty() {
        es.submit()
    }
    //TODO wait ongoing requests to be done, using a channel of channel, with each channel mapping a request and sending a single message to warn it's done
}
