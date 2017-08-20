Elasticsearch Indexing Proxy Plugin
===================================

## Overview

This plugin provides a proxy feature for target indices.
For target indices, this plugin intercepts indexing requests and writes them to files.
Moreover, Document Sender provided by this plugin reads the files and sends them to any indices.

## Version

| Version   | Tested on Elasticsearch |
|:---------:|:-----------------------:|
| master    | 5.5.X                   |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-indexing-proxy/issues "issue").

## Installation

Not released yet.

    $ $ES_HOME/bin/elasticsearch-plugin install org.codelibs:elasticsearch-indexing-proxy:5.5.0

## Getting Started

### Configuration

This plugin has the following settings.

| Setting                               | Type    | Default      | Description                   |
|:--------------------------------------|:-------:|:------------:|:------------------------------|
| idxproxy.data.path                    | string  | es.data.path | directory to store data file  |
| idxproxy.target.indices               | string  | none         | target indices                |
| idxproxy.data.file\_size              | size    | 100m         | file size                     |
| idxproxy.indexer.interval             | time    | 30s          | interval to check file        |
| idxproxy.indexer.retry\_count         | int     | 10           | retry count for error file    |
| idxproxy.indexer.request.retry\_count | int     | 3            | retry count for error request |
| idxproxy.indexer.skip.error\_file     | boolean | true         | skip error file               |

For example, put settings as below:

```
idxproxy.data.path: /opt/elasticsearch/idxproxy-data
idxproxy.target.indices: sample
```

The above settings intercepts indexing(index,update,delete) requests and stores them to /opt/elasticsearch/idxproxy-data/[0-9]+.dat.

### Create Index and Alias

Create sample1 index and sample alias:

```
curl -XPUT 'localhost:9200/sample1?pretty' -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "index" : {
            "number_of_shards" : 3,
            "number_of_replicas" : 0
        }
    }
}
'
curl -XPOST 'localhost:9200/_aliases?pretty' -H 'Content-Type: application/json' -d'
{
    "actions" : [
        { "add" : { "index" : "sample1", "alias" : "sample" } }
    ]
}
'
```

### Send Document

Send 1 document to sample alias:

```
curl -XPOST 'localhost:9200/sample/doc/1?pretty' -H 'Content-Type: application/json' -d'
{
    "msg" : "test 1"
}
'
```

Check sample index:

```
curl 'localhost:9200/sample/_search?q=*:*&pretty'
```

If this plugin works correctly, hits.total is 0.
.tmp file is a working files in idxproxy.data.path directory.

```
$ ls idxproxy-data/
0000000000000000001.tmp
```

### Flush Data file

Send refresh request:

```
curl -XPOST 'localhost:9200/_refresh?pretty'
```

.dat file and new .tmp file are created.

```
ls idxproxy-data/
0000000000000000001.dat		0000000000000000002.tmp
```

### Start Document Sender

Start Document Sender for sample1 index:

```
curl -XPOST 'localhost:9200/sample1/_idxproxy/process?pretty'
```

Check sample index:

```
curl 'localhost:9200/sample/_search?q=*:*&pretty'
```

You will find a sent document.

### Stop Document Sender

Stop Document Sender for sample1 index:

```
curl -XDELETE 'localhost:9200/sample1/_idxproxy/process?pretty'
```

