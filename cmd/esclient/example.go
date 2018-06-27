package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/olivere/elastic"
)

const (
	indexName    = "log"
	typeName     = "log"
	indexMapping = `
	{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "log": {
      "properties": {
        "at": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "at2": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "at3": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "host": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "ip": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "message": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "method": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "path": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "response_time": {
          "type": "long"
        },
        "traceno": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        }
      }
    }
  }
}
`
)

type AccessLogEntry struct {
	Method       string  `json:"method"`
	Host         string  `json:"host"`
	Path         string  `json:"path"`
	IP           string  `json:"ip"`
	ResponseTime float64 `json:"response_time"`
	At           string  `json:"at"`
	At2          string  `json:"at2"`
	At3          string  `json:"at3"`
	TraceNo      string  `json:"traceno"`
	Message      string  `json:"message"`
}

type Finder struct {
	from   int
	size   int
	sort   []string
	pretty bool
}

type FinderResponse struct {
	Total int64
	Logs  []*AccessLogEntry
}

func NewFinder() *Finder {
	return &Finder{}
}

func (f *Finder) From(n int) *Finder {
	f.from = n
	return f
}

func (f *Finder) Size(n int) *Finder {
	f.size = n
	return f
}

func (f *Finder) Pretty(s bool) *Finder {
	f.pretty = s
	return f
}

func (f *Finder) Sort(s ...string) *Finder {
	if f.sort == nil {
		f.sort = make([]string, 0)
	}
	f.sort = append(f.sort, s...)
	return f
}

func (f *Finder) Find(ctx context.Context, client *elastic.Client) (FinderResponse, error) {
	var resp FinderResponse

	search := client.Search().Index(indexName).Type(typeName).Pretty(f.pretty)

	// Execute search
	result, err := search.Do(ctx)
	if err != nil {
		return resp, err
	}

	// Decode response
	acLogs, err := f.decodeResp(result)
	if err != nil {
		return resp, err
	}

	resp.Logs = acLogs
	resp.Total = result.Hits.TotalHits

	return resp, nil
}

func (f *Finder) decodeResp(res *elastic.SearchResult) ([]*AccessLogEntry, error) {
	if res == nil || res.TotalHits() == 0 {
		return nil, nil
	}

	var acLogs []*AccessLogEntry
	for _, hit := range res.Hits.Hits {
		acLog := new(AccessLogEntry)
		if err := json.Unmarshal(*hit.Source, acLog); err != nil {
			return nil, err
		}
		acLogs = append(acLogs, acLog)
	}

	return acLogs, nil
}

func main() {
	opts := []elastic.ClientOptionFunc{
		elastic.SetSniff(false),
		elastic.SetURL("http://localhost:9200"),
	}

	cli, err := elastic.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cli.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f := NewFinder()
	f = f.From(0).Size(100)

	res, err := f.Find(ctx, cli)
	for _, l := range res.Logs {
		fmt.Println(">", l.At, l.At2)
	}

}
