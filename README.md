# elasticsearch-urls

## Overview

```elasticsearch-urls```: pass it elasticsearch host info and an index name and it will return sitemap-urls like data (the url and the publishDate fields in tab-delimited lines) to stdout.

The code assumes a "crawl_export" Article type, which is defined in the ```crawl_export.go`` file.

## Building the tool

```
$ ./build
```

## Examples of Running the tool
```
$ elasticsearch-urls -i myindex > urls.txt      # put all urls to a text file
$ elasticsearch-urls -i myindex | grep "xml$"   # show all files with "xml" at the end
```