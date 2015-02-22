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
}

func NewElasticSearch(server string) *ElasticSearch {
    return &ElasticSearch { &http.Client{}, server }
}


func (es *ElasticSearch) Add(w *wet.WETEntry) error {
    docId := w.Headers["WARC-Record-ID"]

    doc := make(map[string]string)
    doc["id"] = docId
    doc["url"] = w.Headers["WARC-Target-URI"]
    doc["content"] = string(w.Body)
    //TODO don't store _source

    encoded, err := json.Marshal(doc)
    if err == nil {
        url := es.server + "/" + indexName + "/" + indexType + "/" + docId
        var req *http.Request
        req, err = http.NewRequest("PUT", url, bytes.NewReader(encoded))
        if err == nil {
            req.Header.Add("Content-Type", "application/json")
            var res *http.Response
            res, err = es.client.Do(req)
            defer res.Body.Close()
        }
    }

    return err
}
