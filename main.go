// elasticsearch-urls is a program that queries an Elasticsearch index
// and returns all urls and publish dates for any type that mimics my
// site crawl article type.
// This is useful in comparing with sitemap data for comparing
// what the sitemap says a site has vs. what I have in my crawl export index.
package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	esIndex := flag.String("i", "", "the index to check in Elasticsearch")
	esHost := flag.String("h", "localhost", "the Elasticsearch host")
	esPort := flag.String("p", "9200", "the Elasticsearch port")
	pageSize := flag.Int64("s", 1000, "the size of the pages to call Elasticsearch with")

	flag.Parse()
	validateArgs(*esIndex)

	articles, err := retrieveArticles(*esHost, *esPort, *esIndex, *pageSize)

	if err != nil {
		log.Fatal("Problem retrieving crawl export articles:", err)
	}

	for _, article := range articles {
		fmt.Printf("%s\t%s\n", article.URL, article.PublishDate)
	}
}

func validateArgs(esIndex string) {
	if esIndex == "" {
		log.Fatal("elasticsearch Index (-i=<index>) is required")
	}
}

func retrieveArticles(elasticearchHost string, elasticsearchPort string, elasticsearchIndex string, pageSize int64) ([]*Article, error) {
	client := NewCrawlExportClient(elasticearchHost, elasticsearchPort)
	return client.RetrieveEntireSourceContents(elasticsearchIndex, "article", pageSize)
}
