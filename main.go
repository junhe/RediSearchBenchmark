package main

import (
	"flag"
	"fmt"
	"bufio"
        "os"
	"strings"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/index/elastic"
	"github.com/RedisLabs/RediSearchBenchmark/index/redisearch"
	"github.com/RedisLabs/RediSearchBenchmark/index/solr"
	"github.com/RedisLabs/RediSearchBenchmark/ingest"
	"github.com/RedisLabs/RediSearchBenchmark/query"
	"github.com/RedisLabs/RediSearchBenchmark/synth"
)

// IndexName is the name of our index on all engines
const IndexName = "wik"

var indexMetadata = index.NewMetadata().
	AddField(index.NewTextField("body", 1))
	//AddField(index.NewTextField("title", 10)).
	//AddField(index.NewNumericField("score"))

// selectIndex selects and configures the index we are now running based on the engine name, hosts and number of shards
func selectIndex(engine string, hosts []string, partitions int, cmdPrefix string) (index.Index, index.Autocompleter, interface{}) {

	switch engine {
	case "redis":

		//return redisearch.NewIndex(hosts[0], "wik{0}", indexMetadata)
		idx := redisearch.NewDistributedIndex(IndexName, hosts, partitions, indexMetadata)
		return idx, idx, query.QueryVerbatim

	case "redismod":
		indexMetadata.Options = redisearch.IndexingOptions{Prefix: cmdPrefix}
		//return redisearch.NewIndex(hosts[0], "wik{0}", indexMetadata)
		idx := redisearch.NewIndex(hosts[0], "wiki", indexMetadata)
		ac := redisearch.NewAutocompleter(hosts[0], "ac")
		return idx, ac, query.QueryVerbatim

	case "elastic":
		idx, err := elastic.NewIndex(hosts[0], IndexName, "doc", indexMetadata)
		if err != nil {
			panic(err)
		}
		fmt.Println("after get newindex====");
                return idx, idx, 0
	case "solr":
		idx, err := solr.NewIndex(hosts[0], IndexName, indexMetadata)
		if err != nil {
			panic(err)
		}
		return idx, idx, 0

	}
	panic("could not find index type " + engine)
}
func loadQueryPool(file_path string) ([]string){
        fmt.Println("Query Pool Path: ", file_path)
        // load document
        var querypool []string
        file, err := os.Open(file_path)
        if err != nil {
            panic(err)
        }
        defer file.Close()

        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
            //fmt.Println(scanner.Text())
            querypool = append(querypool, scanner.Text())
        }

        if err := scanner.Err(); err != nil {
            panic(err)
        }
        return querypool
}

func main() {

	hosts := flag.String("hosts", "localhost:6379", "comma separated list of host:port to redis nodes")
	partitions := flag.Int("shards", 1, "the number of partitions we want (AT LEAST the number of cluster shards)")
	fileName := flag.String("file", "", "Input file to ingest data from (wikipedia abstracts)")
	scoreFile := flag.String("scores", "", "read scores of documents CSV for indexing")
	engine := flag.String("engine", "redis", "The search backend to run")
	benchmark := flag.String("benchmark", "", "[search|suggest] - if set, we run the given benchmark")
	random := flag.Int("random", 0, "Generate random documents with terms like term0..term{N}")
	fuzzy := flag.Bool("fuzzy", false, "For redis only - benchmark fuzzy auto suggest")
	seconds := flag.Int("duration", 100, "number of seconds to run the benchmark")
	conc := flag.Int("c", 4, "benchmark concurrency")
	qs := flag.String("queries", "hello world", "comma separated list of queries to benchmark")
	outfile := flag.String("o", "benchmark.csv", "results output file. set to - for stdout")
	duration := time.Second * time.Duration(*seconds)
	cmdPrefix := flag.String("prefix", "FT", "Command prefix for FT module")
        querypath := flag.String("querypath", "", "Query pool for benchmark")

	flag.Parse()
	servers := strings.Split(*hosts, ",")
	if len(servers) == 0 {
		panic("No servers given")
	}

        var queries []string
        if *querypath == "" {
	        queries = strings.Split(*qs, ",")
        } else {
                queries = loadQueryPool(*querypath)
                fmt.Println("Overall ", len(queries), " queries")
        }

	// select index to run
	idx, ac, opts := selectIndex(*engine, servers, *partitions, *cmdPrefix)

	// Search benchmark
	if *benchmark == "search" {
                var name_str string
                name_str = *qs
                if *querypath != "" {
                        name_str = *querypath
                }
                name := fmt.Sprintf("search: %s %d", name_str, len(queries))
                //Benchmark(*conc, duration, *engine, name, *outfile, SearchBenchmark(queries, querytype, idx, opts))
                Benchmark(*conc, duration, *engine, name, *outfile, SearchBenchmark(queries, idx, opts))
		os.Exit(0)
	}

	// Auto-suggest benchmark
	if *benchmark == "suggest" {
		Benchmark(*conc, duration, *engine, "suggest", *outfile, AutocompleteBenchmark(ac, *fuzzy))
		os.Exit(0)
	}

	// ingest random documents
	if *random > 0 {
		idx.Drop()
		idx.Create()

		N := 1000
		gen := synth.NewDocumentGenerator(*random, map[string][2]int{"title": {5, 10}, "body": {10, 20}})
		chunk := make([]index.Document, N)
		n := 0
		ch := make(chan index.Document, N)
		go func() {
			for {
				ch <- gen.Generate(0)
			}
		}()
		for {

			for i := 0; i < N; i++ {
				chunk[i] = <-ch
				//fmt.Println(chunk[i])
				n++
			}

			idx.Index(chunk, nil)
			fmt.Println(n)
		}

	}
	// ingest documents into the selected engine
	if *fileName != "" && *benchmark == "" {
		fmt.Println("Prepare to index...")
                if ac != nil {
			ac.Delete()
		}

		idx.Drop()
		idx.Create()
		wr := ingest.NewWikipediaAbstractsReader()

		if *scoreFile != "" {
			if err := wr.LoadScores(*scoreFile); err != nil {
				panic(err)
			}
		}
                fmt.Println("===== Prepare to ingest")
		if err := ingest.IngestDocuments(*fileName, wr, idx, ac, redisearch.IndexingOptions{NoSave: false,
			NoOffsetVectors: true}, 1000); err != nil {
			panic(err)
		}

		os.Exit(0)
	}

	fmt.Fprintln(os.Stderr, "No benchmark or input file specified")
	flag.Usage()
	os.Exit(-1)
}
