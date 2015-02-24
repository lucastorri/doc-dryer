


## Workspace

On a structure like:

```
$GOPATH
└── src
    └── github.com
        └── lucastorri
            └── doc-dryer
                └── doc-dryer.go
                └── ...
```

```
go build github.com/lucastorri/doc-dryer

go build github.com/lucastorri/doc-dryer && time ./doc-dryer -publish urls.txt
```

http://www.elasticsearch.org/blog/tribe-node/
