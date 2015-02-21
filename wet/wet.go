package wet

import (
    "os"
    "compress/gzip"
    "bufio"
    "regexp"
    "strconv"
    "strings"
)

type WETEntry struct {
    IsHeader bool
    Version string
    Headers map[string]string
    Body []byte
}

type Stringer interface {
    String() string
}

type WETReader struct {
    reader *bufio.Reader
    header *WETEntry
}

func (reader *WETReader) Header() *WETEntry {
    return reader.header
}

func (reader *WETReader) Next() (*WETEntry, error) {
    return reader.extractEntry(false)
}

func FromGZip(file string) (wr *WETReader, err error) {
    fr, err := os.Open(file)
    if err != nil {
        return
    }
    gzr, err := gzip.NewReader(fr)
    if err != nil {
        return
    }
    buf := bufio.NewReader(gzr)
    wr = &WETReader { buf, nil }
    err = wr.init()
    return
}

func (wet *WETReader) init() (err error) {
    header, err := wet.extractEntry(true)
    if err != nil {
        return
    }
    wet.header = header
    return
}

func (wet *WETReader) extractEntry(isHeader bool) (entry *WETEntry, err error) {
    defer func() {
        r := recover()
        if r != nil {
            err = r.(error)
        }
    }()
    version := parseVersion(wet.nextLine())
    headers := parseHeaders(wet)

    bodyLength, err := strconv.Atoi(headers["Content-Length"])
    if err != nil {
        return
    }
    body := parseBody(bodyLength, wet.reader)
    wet.nextLine()
    wet.nextLine()

    entry = &WETEntry { isHeader, version, headers, body }
    return
}

func (wet *WETReader) nextLine() string {
    line, err := wet.reader.ReadString(byte('\n'))
    if err != nil {
        panic(err)
    }
    return strings.TrimSpace(string(line))
}

var versionRegex = regexp.MustCompile("WARC/(.*)")
var headerRegex = regexp.MustCompile("([^:]+): (.*)")

func parseVersion(line string) string {
    return versionRegex.FindStringSubmatch(line)[1]
}

func parseHeaders(wet *WETReader) (headers map[string]string) {
    headers = make(map[string]string)

    line := wet.nextLine()
    for line != "" {
        match := headerRegex.FindStringSubmatch(line)
        headers[match[1]] = match[2]
        line = wet.nextLine()
    }
    return
}

func parseBody(length int, reader *bufio.Reader) (body []byte) {
    body = make([]byte, length)
    total := 0
    for total < length {
        read, err := reader.Read(body[total:])
        if err != nil {
            panic(err)
        }
        total += read
    }
    return
}
