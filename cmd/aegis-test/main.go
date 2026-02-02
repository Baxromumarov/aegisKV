// aegis-test is a testing tool for AegisKV cluster
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/client"
)

func main() {
	var addrs string
	var numOps int
	var numWorkers int
	var keyPrefix string
	var testType string

	flag.StringVar(&addrs, "addrs", "localhost:7700", "Comma-separated list of seed nodes")
	flag.IntVar(&numOps, "ops", 1000, "Number of operations")
	flag.IntVar(&numWorkers, "workers", 10, "Number of concurrent workers")
	flag.StringVar(&keyPrefix, "prefix", "test-", "Key prefix")
	flag.StringVar(&testType, "test", "all", "Test type: connectivity, set, get, all, benchmark")
	flag.Parse()

	seedList := []string{}
	for _, s := range splitSeeds(addrs) {
		seedList = append(seedList, s)
	}

	fmt.Println("=== AegisKV Cluster Test ===")
	fmt.Printf("Addrs: %v\n", seedList)
	fmt.Printf("Operations: %d\n", numOps)
	fmt.Printf("Workers: %d\n", numWorkers)
	fmt.Printf("Test type: %s\n", testType)
	fmt.Println()

	c := client.New(client.Config{
		Addrs:        seedList,
		MaxConns:     numWorkers * 2,
		ConnTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxRetries:   5,
	})
	defer c.Close()

	switch testType {
	case "connectivity":
		testConnectivity(c, seedList)
	case "set":
		testSet(c, numOps, keyPrefix)
	case "get":
		testGet(c, numOps, keyPrefix)
	case "benchmark":
		runBenchmark(c, numOps, numWorkers, keyPrefix)
	case "all":
		testConnectivity(c, seedList)
		testSet(c, numOps, keyPrefix)
		testGet(c, numOps, keyPrefix)
	default:
		log.Fatalf("Unknown test type: %s", testType)
	}

	fmt.Println("\n=== Test Complete ===")
}

func splitSeeds(addrs string) []string {
	result := []string{}
	current := ""
	for _, c := range addrs {
		if c == ',' {
			if current != "" {
				result = append(result, current)
			}
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

func testConnectivity(c *client.Client, addrs []string) {
	_ = c
	fmt.Println("--- Testing Connectivity ---")

	for _, seed := range addrs {
		// Try to do a simple operation against each seed
		testClient := client.New(client.Config{
			Addrs:       []string{seed},
			ConnTimeout: 2 * time.Second,
			MaxRetries:  1,
		})

		key := fmt.Sprintf("connectivity-test-%s-%d", seed, time.Now().UnixNano())
		err := testClient.SetWithTTL([]byte(key), []byte("test"), 60*time.Second)
		testClient.Close()

		if err != nil {
			fmt.Printf("  %s: FAILED (%v)\n", seed, err)
		} else {
			fmt.Printf("  %s: OK\n", seed)
		}
	}
	fmt.Println()
}

func testSet(c *client.Client, numOps int, prefix string) {
	fmt.Println("--- Testing SET Operations ---")

	success := 0
	failed := 0

	start := time.Now()
	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		value := fmt.Sprintf("value-%d-%d", i, time.Now().UnixNano())

		err := c.SetWithTTL([]byte(key), []byte(value), 5*time.Minute)
		if err != nil {
			failed++
			if failed <= 5 {
				fmt.Printf("  SET %s failed: %v\n", key, err)
			}
		} else {
			success++
		}
	}
	elapsed := time.Since(start)

	fmt.Printf("  SET Results: %d/%d success (%.1f%%)\n", success, numOps, float64(success)*100/float64(numOps))
	fmt.Printf("  Throughput: %.1f ops/sec\n", float64(success)/elapsed.Seconds())
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Println()
}

func testGet(c *client.Client, numOps int, prefix string) {
	fmt.Println("--- Testing GET Operations ---")

	// First set the keys
	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		value := fmt.Sprintf("value-%d", i)
		c.SetWithTTL([]byte(key), []byte(value), 5*time.Minute)
	}

	success := 0
	failed := 0
	notFound := 0

	start := time.Now()
	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)

		_, err := c.Get([]byte(key))
		if err != nil {
			if err == client.ErrNotFound {
				notFound++
			} else {
				failed++
				if failed <= 5 {
					fmt.Printf("  GET %s failed: %v\n", key, err)
				}
			}
		} else {
			success++
		}
	}
	elapsed := time.Since(start)

	fmt.Printf("  GET Results: %d/%d success (%.1f%%)\n", success, numOps, float64(success)*100/float64(numOps))
	fmt.Printf("  Not found: %d\n", notFound)
	fmt.Printf("  Errors: %d\n", failed)
	fmt.Printf("  Throughput: %.1f ops/sec\n", float64(success)/elapsed.Seconds())
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Println()
}

func runBenchmark(c *client.Client, numOps int, numWorkers int, prefix string) {
	fmt.Println("--- Running Benchmark ---")

	var setSuccess, setFailed int64
	var getSuccess, getFailed int64

	// SET Benchmark
	fmt.Println("SET Benchmark...")
	start := time.Now()
	var wg sync.WaitGroup
	opsPerWorker := numOps / numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("%s%d-%d", prefix, workerID, i)
				value := fmt.Sprintf("benchmark-value-%d-%d", workerID, i)

				err := c.SetWithTTL([]byte(key), []byte(value), 5*time.Minute)
				if err != nil {
					atomic.AddInt64(&setFailed, 1)
				} else {
					atomic.AddInt64(&setSuccess, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	setDuration := time.Since(start)

	fmt.Printf("  SET: %d/%d success, %.1f ops/sec\n",
		setSuccess, numOps, float64(setSuccess)/setDuration.Seconds())

	// GET Benchmark
	fmt.Println("GET Benchmark...")
	start = time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("%s%d-%d", prefix, workerID, i)

				_, err := c.Get([]byte(key))
				if err != nil {
					atomic.AddInt64(&getFailed, 1)
				} else {
					atomic.AddInt64(&getSuccess, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	getDuration := time.Since(start)

	fmt.Printf("  GET: %d/%d success, %.1f ops/sec\n",
		getSuccess, numOps, float64(getSuccess)/getDuration.Seconds())

	// Mixed workload
	fmt.Println("Mixed Workload (80% GET, 20% SET)...")
	var mixedSuccess, mixedFailed int64
	start = time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("%s%d-%d", prefix, workerID, rng.Intn(opsPerWorker))
				var err error
				if rng.Float64() < 0.8 {
					_, err = c.Get([]byte(key))
				} else {
					err = c.SetWithTTL([]byte(key), []byte("mixed-value"), 5*time.Minute)
				}
				if err != nil {
					atomic.AddInt64(&mixedFailed, 1)
				} else {
					atomic.AddInt64(&mixedSuccess, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	mixedDuration := time.Since(start)

	fmt.Printf("  Mixed: %d/%d success, %.1f ops/sec\n",
		mixedSuccess, numOps, float64(mixedSuccess)/mixedDuration.Seconds())

	fmt.Println()
	fmt.Println("--- Benchmark Summary ---")
	fmt.Printf("  SET Throughput: %.1f ops/sec\n", float64(setSuccess)/setDuration.Seconds())
	fmt.Printf("  GET Throughput: %.1f ops/sec\n", float64(getSuccess)/getDuration.Seconds())
	fmt.Printf("  Mixed Throughput: %.1f ops/sec\n", float64(mixedSuccess)/mixedDuration.Seconds())
	fmt.Println()
}
