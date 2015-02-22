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
    Version string
    Headers map[string]string
    Body []byte
}

type WETReader struct {
    reader *bufio.Reader
    gzip *gzip.Reader
    header *WETEntry
}

/**
 * Builds a new WETReader from a GZip file, or returns an error if cannot process
 * the given file
 */
func FromGZip(file string) (wr *WETReader, err error) {
    if fr, err := os.Open(file); err == nil {
        if gzr, err := gzip.NewReader(fr); err == nil {
            wr = &WETReader { bufio.NewReader(gzr), gzr, nil }
            err = wr.init()
        }
    }
    return
}

/**
 * Returns the header describing this WET file
 */
func (reader *WETReader) Header() *WETEntry {
    return reader.header
}

/**
 * Returns the next entry on the WET file, or an error if could not parse it. If
 * the end of the file was reached, return an io.EOF error.
 */
func (wet *WETReader) Channel() (<-chan struct { Entry *WETEntry; Err error }) {
    channel := make(chan struct { Entry *WETEntry; Err error })
    go func() {
        defer func() {
            wet.Close()
            close(channel)
        }()
        for {
            entry, err := wet.extractEntry()
            channel <- struct { Entry *WETEntry; Err error }{ entry, err }
            if err != nil {
                return
            }
        }
    }()
    return channel
}

func (wet *WETReader) Close() {
    wet.gzip.Close()
}

func (wet *WETReader) init() (err error) {
    header, err := wet.extractEntry()
    if err != nil {
        return
    }
    wet.header = header
    return
}

var versionRegex = regexp.MustCompile("WARC/(.*)")
var headerRegex = regexp.MustCompile("([^:]+): (.*)")

func (wet *WETReader) extractEntry() (entry *WETEntry, err error) {
    defer func() {
        r := recover()
        if r != nil {
            err = r.(error)
        }
    }()
    version := wet.parseVersion()
    headers := wet.parseHeaders()

    bodyLength, err := strconv.Atoi(headers["Content-Length"])
    if err != nil {
        return
    }
    body := wet.parseBody(bodyLength)
    wet.nextLine()
    wet.nextLine()

    entry = &WETEntry { version, headers, body }
    return
}

func (wet *WETReader) nextLine() string {
    line, err := wet.reader.ReadString(byte('\n'))
    if err != nil {
        panic(err)
    }
    return strings.TrimSpace(string(line))
}

func (wet *WETReader) parseVersion() string {
    line := wet.nextLine()
    return versionRegex.FindStringSubmatch(line)[1]
}

func (wet *WETReader) parseHeaders() (headers map[string]string) {
    headers = make(map[string]string)
    line := wet.nextLine()
    for line != "" {
        match := headerRegex.FindStringSubmatch(line)
        headers[match[1]] = match[2]
        line = wet.nextLine()
    }
    return
}

func (wet *WETReader) parseBody(length int) (body []byte) {
    body = make([]byte, length)
    total := 0
    for total < length {
        read, err := wet.reader.Read(body[total:])
        if err != nil {
            panic(err)
        }
        total += read
    }
    return
}
