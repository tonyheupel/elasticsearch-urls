package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
	"time"
)

// An Article represents a normalized structure for web pages
// that are considered articles.
type Article struct {
	ID              string    `json:"id"`
	URL             string    `json:"url"`
	PublishDate     string    `json:"publishDate"`
	Title           string    `json:"title"`
	Content         string    `json:"content"`
	Host            string    `json:"host"`
	Description     string    `json:"description"`
	MetaKeywords    string    `json:"metaKeywords"`
	PreviewImageURL string    `json:"previewImageUrl"`
	Digest          string    `json:"digest"`
	Boost           string    `json:"boost"`
	Timestamp       time.Time `json:"tstamp"`
}

// The CrawlExportClient represents the client a caller uses to
// make calls into the crawl export repository.
type CrawlExportClient struct {
	Host string
	Port string
}

// NewCrawlExportClient creates a new client for the given host and port.
func NewCrawlExportClient(host string, port string) *CrawlExportClient {
	return &CrawlExportClient{
		Host: host,
		Port: port,
	}
}

// RetrieveEntireSourceContents will retrieve all documents for a given source (e.g., popsci)
// for a given topic (e.g., article).  This is an expensive operation, so use wisely.
func (c *CrawlExportClient) RetrieveEntireSourceContents(source string, topic string, pageSize int64) ([]*Article, error) {
	connection := elastigo.NewConn()
	connection.Domain = c.Host
	connection.SetPort(c.Port)

	size := fmt.Sprintf("%d", pageSize)
	scrollTimeOpen := "30s"

	var results *elastigo.SearchResult

	results, err := elastigo.Search(source).
		Type(topic).
		Size(size).
		Fields("url", "publishDate").
		Scroll(scrollTimeOpen).
		Result(connection)

	if err != nil {
		return nil, err
	}

	var allArticles []*Article
	processResults := true

	for processResults {
		articles, err := parseArticlesFromSearchResults(results)

		if err != nil {
			return nil, err
		}

		allArticles = append(allArticles, articles...)

		if results.ScrollId != "" && results.Hits.Len() > 0 {
			scrollResults, err := connection.Scroll(map[string]interface{}{"scroll": scrollTimeOpen}, results.ScrollId)

			if err != nil {
				processResults = false
				return nil, err
			} else {
				results.ScrollId = ""
				results = &scrollResults
			}
		} else {
			processResults = false
		}
	}

	return allArticles, nil
}

// parseArticlesFromSearchResults is a helper function that takes an elasticsearch SearchResults
// and converts the Source of each result item and convert it to an article, and then returns
// all of the articles.
func parseArticlesFromSearchResults(results *elastigo.SearchResult) ([]*Article, error) {
	total := len(results.Hits.Hits)
	articles := make([]*Article, total)

	for i, hit := range results.Hits.Hits {
		//article, err := newArticleFromHitSource(hit.Source)
		article, err := newArticleFromHitFields(hit.Fields)
		if err != nil {
			return nil, err
		}

		articles[i] = article
	}

	return articles, nil
}

// newArticleFromHitFields is a helper function that converts an elasticsearch search
// result hit's Fields property into a strongly-typed Article for the expected fields.
func newArticleFromHitFields(hitFields *json.RawMessage) (*Article, error) {
	fields, err := json.Marshal(hitFields)

	if err != nil {
		return nil, err
	}

	fieldsReader := bytes.NewReader(fields)
	var articleFields map[string][]interface{}

	err = json.NewDecoder(fieldsReader).Decode(&articleFields)

	if err != nil {
		return nil, err
	}

	var article Article
	for key, decodedFieldValue := range articleFields {
		value := decodedFieldValue[0]
		switch value := value.(type) {
		case string:
			if key == "url" {
				article.URL = value
			} else if key == "publishDate" {
				article.PublishDate = value
			}
		default:
			// Unexpected type, do nothing
		}
	}

	return &article, nil
}

// newArticleFromHitSource is a helper function that converts an elasticsearch search
// result hit's Source property into a strongly-typed Article.
func newArticleFromHitSource(hitSource *json.RawMessage) (*Article, error) {
	source, err := json.Marshal(hitSource)

	if err != nil {
		return nil, err
	}

	sourceReader := bytes.NewReader(source)
	var article Article
	err = json.NewDecoder(sourceReader).Decode(&article)

	if err != nil {
		return nil, err
	}

	return &article, nil
}
