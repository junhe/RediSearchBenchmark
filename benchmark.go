package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
        "strconv"
        "strings"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

var latencyPool [100000]int     // 0-100000 0.1ms
var longtail []float64
// SearchBenchmark returns a closure of a function for the benchmarker to run, using a given index
// and options, on a set of queries
func SearchBenchmark(queries []string, idx index.Index, opts interface{}) func(int) error {
	counter := 0
	return func(client_id int) error {
		q := query.NewQuery(IndexName, 
			queries[(client_id + num_clients  * counter) % len(queries)]).Limit(0, 5)
		st := time.Now()
                //_, took, err := idx.Search(*q)  //Single Query, cares about the latency
                _, _, err := idx.Search(*q)     //Multiuple queries
                latency := time.Since(st).Nanoseconds()/100000   //Multiple queries
                //latency = int64(took)                 // Single Query 
                if latency > 99999 {
                        //fmt.Println("======= long tail: ", float64(latency)/10, " ms")
		        longtail = append(longtail, float64(latency)/10)
                } else {
                       latencyPool[latency] += 1
                }
                counter++
		return err
	}
}

// AutocompleteBenchmark returns a configured autocomplete benchmarking function to be run by
// the benchmarker
func AutocompleteBenchmark(ac index.Autocompleter, fuzzy bool) func(int) error {
	counter := 0
	sz := len(prefixes)
	return func(client_id int) error {
		_, err := ac.Suggest(prefixes[rand.Intn(sz)], 5, fuzzy)
		counter++
		return err
	}
}

// Benchmark runs a given function f for the given duration, and outputs the throughput and latency of the function.
//
// It receives metadata like the engine we are running and the title of the specific benchmark, and writes these along
// with the results to a CSV file given by outfile.
//
// If outfile is "-" we write the result to stdout
func Benchmark(concurrency int, duration time.Duration, engine, title string, outfile string, f func(int) error) {

	var out io.WriteCloser
	var err error
	if outfile == "-" {
		out = os.Stdout
	} else {
		out, err = os.OpenFile(outfile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0665)
		if err != nil {
			panic(err)
		}
		defer out.Close()
	}

	startTime := time.Now()
	// the total time it took to run the functions, to measure average latency, in nanoseconds
	var totalTime uint64
	var total uint64
	wg := sync.WaitGroup{}

	//end := time.Now().Add(duration)
        querlog_length, _:= strconv.Atoi(strings.Fields(title)[2])
        max_queries := uint64(querlog_length)
        // uint64(100000)   // TODO
        //max_queries := uint64(1)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(client_id int) { // pass in client_id = i
			//for time.Now().Before(end) {
			for ; total < max_queries; {
				atomic.AddUint64(&total, 1)

				tst := time.Now()

				if err = f(client_id); err != nil {
					panic(err)
				}

				// update the total requests performed and total time
				atomic.AddUint64(&totalTime, uint64(time.Since(tst)))

			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	avgLatency := (float64(totalTime) / float64(total)) / float64(time.Millisecond)
	rate := float64(total) / (float64(time.Since(startTime)) / float64(time.Second))
        // get median/95/99 latency
        pos_median := int(total/2)
        pos_95 := int(float64(total)/100*95)
        pos_99 := int(float64(total)/100*99)

        pos := 0
        lat_median := 0.0
        lat_95 := 0.0
        lat_99 := 0.0
        for i:=0; i < 100000; i++ {
                if latencyPool[i] == 0 {
                    continue
                }
                pos += latencyPool[i]
                if pos >= pos_median {
                    if lat_median == 0.0 {
                            lat_median = float64(i)/10
                    }
                    if pos >= pos_95 {
                            if lat_95 == 0.0 {
                                    lat_95 = float64(i)/10
                            }
                            if pos >= pos_99 {
                                    if lat_99 == 0.0 {
                                            lat_99 = float64(i)/10
                                            break
                                    }
                            }
                    }
                }
        }
        fmt.Print("Duration: ", float64(total) / rate, "\n")
        fmt.Print("Throughput: ", rate, "\n")
        fmt.Print("Latencies: ", lat_median, ", ", lat_95, ", ", lat_99, "\n")
        fmt.Print("Positions: ", pos_median, ", ", pos_95, ", ", pos_99, "\n")
	// Output the results to CSV
	w := csv.NewWriter(out)
	err = w.Write([]string{engine, title,
		fmt.Sprintf("%d", concurrency),
		fmt.Sprintf("%.02f", rate),
		fmt.Sprintf("%.02f", avgLatency),
		fmt.Sprintf("%d", total),
		fmt.Sprintf("%.02f", lat_median),
		fmt.Sprintf("%.02f", lat_95),
		fmt.Sprintf("%.02f", lat_99),
                })

        //for i:=0; i < 100000; i++ {
        //       if latencyPool[i]!=0 {
        //               for j:=0; j < latencyPool[i]; j++ {
        //                   fmt.Print(float32(i)/10, ",")
        //               }
        //       }
        //}
        for i:=0; i< len(longtail); i++ {
               fmt.Println("Long tail:")
               fmt.Print(longtail[i], ",")
        }

        fmt.Println()
        fmt.Print("Total: ", total, " Queries")
        //fmt.Println(latencyPool)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing: %s\n", err)
	} else {
		fmt.Println("Done!")
		w.Flush()
	}

}
