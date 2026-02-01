package main

import (
	"fmt"
	"log"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/client"
)

func main() {
	c := client.New(client.Config{
		Seeds:        []string{"localhost:7700"},
		MaxConns:     10,
		ConnTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxRetries:   3,
	})
	defer c.Close()

	key := []byte("hello")
	value := []byte("world")

	fmt.Println("Setting key 'hello' = 'world'...")
	if err := c.Set(key, value); err != nil {
		log.Fatalf("Failed to set: %v", err)
	}
	fmt.Println("Set successful!")

	fmt.Println("\nGetting key 'hello'...")
	result, err := c.Get(key)
	if err != nil {
		log.Fatalf("Failed to get: %v", err)
	}
	fmt.Printf("Got: %s\n", string(result))

	fmt.Println("\nSetting key with TTL...")
	if err := c.SetWithTTL([]byte("temp-key"), []byte("temp-value"), 10*time.Second); err != nil {
		log.Fatalf("Failed to set with TTL: %v", err)
	}
	fmt.Println("Set with TTL successful!")

	fmt.Println("\nGetting key with TTL...")
	result, ttl, err := c.GetWithTTL([]byte("temp-key"))
	if err != nil {
		log.Fatalf("Failed to get with TTL: %v", err)
	}
	fmt.Printf("Got: %s, TTL: %v\n", string(result), ttl)

	fmt.Println("\nDeleting key 'hello'...")
	if err := c.Delete(key); err != nil {
		log.Fatalf("Failed to delete: %v", err)
	}
	fmt.Println("Delete successful!")

	fmt.Println("\nTrying to get deleted key...")
	_, err = c.Get(key)
	if err == client.ErrNotFound {
		fmt.Println("Key not found (expected)")
	} else if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}

	fmt.Println("\n--- Benchmark ---")
	numOps := 10000

	start := time.Now()
	for i := 0; i < numOps; i++ {
		k := []byte(fmt.Sprintf("bench-key-%d", i))
		v := []byte(fmt.Sprintf("bench-value-%d", i))
		if err := c.Set(k, v); err != nil {
			log.Printf("Set failed: %v", err)
		}
	}
	setDuration := time.Since(start)
	fmt.Printf("SET: %d ops in %v (%.0f ops/sec)\n", numOps, setDuration, float64(numOps)/setDuration.Seconds())

	start = time.Now()
	for i := 0; i < numOps; i++ {
		k := []byte(fmt.Sprintf("bench-key-%d", i))
		if _, err := c.Get(k); err != nil && err != client.ErrNotFound {
			log.Printf("Get failed: %v", err)
		}
	}
	getDuration := time.Since(start)
	fmt.Printf("GET: %d ops in %v (%.0f ops/sec)\n", numOps, getDuration, float64(numOps)/getDuration.Seconds())

	fmt.Println("\nExample complete!")
}
