package elastic

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
        "fmt"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
	"gopkg.in/olivere/elastic.v3"
)

// Index is an ElasticSearch index
type Index struct {
	conn *elastic.Client

	md   *index.Metadata
	name string
	typ  string
}

// NewIndex creates a new elasticSearch index with the given address and name. typ is the entity type
func NewIndex(addr, name, typ string, md *index.Metadata) (*Index, error) {

	fmt.Println("Get a new index: ", addr, name)
        client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 200,
		},
		Timeout: 250 * time.Millisecond,
	}
	conn, err := elastic.NewClient(elastic.SetURL(addr), elastic.SetHttpClient(client))
	if err != nil {
                fmt.Println("Get error here");
		return nil, err
	}
	ret := &Index{
		conn: conn,
		md:   md,
		name: name,
		typ:  typ,
	}

	return ret, nil

}

type mappingProperty map[string]interface{}

type mapping struct {
	Properties map[string]mappingProperty `json:"properties"`
}

// convert a fieldType to elastic mapping type string
func fieldTypeString(f index.FieldType) (string, error) {
	switch f {
	case index.TextField:
		return "string", nil
	case index.NumericField:
		return "double", nil
	default:
		return "", errors.New("Unsupported field type")
	}
}

// Create creates the index and posts a mapping corresponding to our Metadata
func (i *Index) Create() error {

	doc := mapping{Properties: map[string]mappingProperty{}}
	for _, f := range i.md.Fields {
		doc.Properties[f.Name] = mappingProperty{}
		fs, err := fieldTypeString(f.Type)
		if err != nil {
			return err
		}
		doc.Properties[f.Name]["type"] = fs
	}

        // Added for apple to apple benchmark
        doc.Properties["body"]["type"] = "text"
        doc.Properties["body"]["analyzer"] = "my_english_analyzer"
        doc.Properties["body"]["search_analyzer"] = "whitespace"
        //doc.Properties["body"]["test"] = "test"
        index_map := map[string]int{
              "number_of_shards" : 2,
              "number_of_replicas" : 1,
        }
        analyzer_map := map[string]interface{}{
                  "my_english_analyzer": map[string]interface{}{
                      "tokenizer":  "standard",
                      "char_filter":  []string{ "html_strip" } ,
                      "filter" : []string{"english_possessive_stemmer", 
                                  "lowercase", "english_stop", 
                                  "english_stemmer", 
                                  "asciifolding", "icu_folding"},
                  },
              }
        filter_map := map[string]interface{}{
                  "english_stop": map[string]interface{}{
                        "type":       "stop",
                        "stopwords":  "_english_",
                  },
                  "english_possessive_stemmer": map[string]interface{}{
                        "type":       "stemmer",
                        "language":   "possessive_english",
                  },
                  "english_stemmer" : map[string]interface{}{
                        "type" : "stemmer",
                        "name" : "english",
                  },
                  "my_folding": map[string]interface{}{
                        "type": "asciifolding",
                        "preserve_original": "false",
                  },
              }
        analysis_map := map[string]interface{}{
              "analyzer": analyzer_map,
              "filter"  : filter_map,
        }
        settings := map[string]interface{}{
               "index": index_map,
               "analysis": analysis_map,
        }

        // TODO delete?
	// we currently manually create the autocomplete mapping
	/*ac := mapping{
		Properties: map[string]mappingProperty{
			"sugg": mappingProperty{
				"type":     "completion",
				"payloads": true,
			},
		},
	}*/

	mappings := map[string]mapping{
		i.typ:          doc,
                //	"autocomplete": ac,
	}

        fmt.Println(mappings)

	//_, err := i.conn.CreateIndex(i.name).BodyJson(map[string]interface{}{"mappings": mappings}).Do()
	_, err := i.conn.CreateIndex(i.name).BodyJson(map[string]interface{}{"mappings": mappings, "settings": settings}).Do()

        if err != nil {
                fmt.Println("Error ", err)
		fmt.Println("!!!!Get Error when using client to create index")
	}

	return err
}

// Index indexes multiple documents
func (i *Index) Index(docs []index.Document, opts interface{}) error {

	blk := i.conn.Bulk()

	for _, doc := range docs {
                //fmt.Println(doc.Properties)
		req := elastic.NewBulkIndexRequest().Index(i.name).Type("doc").Id(doc.Id).Doc(doc.Properties)
		blk.Add(req)

	}
	_, err := blk.Refresh(true).Do()

	return err
}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
func (i *Index) Search(q query.Query) ([]index.Document, int, error) {
        Flag_highlight := true
	//eq := elastic.NewQueryStringQuery(q.Term)
	eq := elastic.NewMatchQuery("body", q.Term)

        // Specify highlighter
        hl := elastic.NewHighlight()
        hl = hl.Fields(elastic.NewHighlighterField("body"))
        hl = hl.PreTags("<em>").PostTags("</em>")
        //src, err := hl.Source()
        //j_src, _ := json.MarshalIndent(&src, "", "   ")
        //fmt.Println(string(j_src))
        //fmt.Println("offset: ", q.Paging.Offset, "max size: ", q.Paging.Num)
        var res *elastic.SearchResult
        var err error
        if Flag_highlight == true {
                res, err = i.conn.Search(i.name).Type("doc").
                Query(eq).
		Highlight(hl).
		From(q.Paging.Offset).
		Size(q.Paging.Num).
		Do()
        } else {
                res, err = i.conn.Search(i.name).Type("doc").
                Query(eq).
		From(q.Paging.Offset).
		Size(q.Paging.Num).
		Do()
        }

        //j, _ := json.MarshalIndent(&res, "", "   ")
        //fmt.Println(string(j))

	if err != nil {
		return nil, 0, err
	}

	ret := make([]index.Document, 0, q.Paging.Num)
	for _, h := range res.Hits.Hits {
		if h != nil {
			d := index.NewDocument(h.Id, float32(*h.Score))
			if err := json.Unmarshal(*h.Source, &d.Properties); err == nil {
				ret = append(ret, d)
			}
		}

	}

	return ret, int(res.TotalHits()), err
}

// Drop deletes the index
func (i *Index) Drop() error {
	i.conn.DeleteIndex(i.name).Do()

	return nil
}

// AddTerms add suggestion terms to the suggester index
func (i *Index) AddTerms(terms ...index.Suggestion) error {
	blk := i.conn.Bulk()

	for _, term := range terms {
		req := elastic.NewBulkIndexRequest().Index(i.name).Type("autocomplete").
			Doc(map[string]interface{}{"sugg": term.Term})

		blk.Add(req)

	}
	_, err := blk.Refresh(true).Do()

	return err

}

// Suggest gets completion suggestions for a given prefix.
// TODO: fuzzy not supported yet
func (i *Index) Suggest(prefix string, num int, fuzzy bool) ([]index.Suggestion, error) {

	s := elastic.NewCompletionSuggester("autocomplete").Field("sugg").Text(prefix).Size(num)

	res, err := i.conn.Suggest(i.name).Suggester(s).Do()
	if err != nil {
		return nil, err
	}

	if suggs, found := res["autocomplete"]; found {
		if len(suggs) > 0 {
			opts := suggs[0].Options

			ret := make([]index.Suggestion, 0, len(opts))
			for _, op := range opts {
				ret = append(ret, index.Suggestion{Term: op.Text, Score: float64(op.Score)})
			}
			return ret, nil
		}

	}

	//ret := make([]index.Suggestion, res.)
	return nil, err

}

// Delete the suggestion index, currently just calls Drop()
func (i *Index) Delete() error {
	return i.Drop()
}
