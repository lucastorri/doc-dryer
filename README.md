


## Workspace

On a structure like:

```
$GOPATH
└── src
    └── here.com
        └── scrooge
            └── doc-dryer
                └── doc-dryer.go
                └── ...
```

```
go build here.com/scrooge/doc-dryer

go build here.com/scrooge/doc-dryer && time ./doc-dryer -publish urls.txt
```
