package main

import (
	"bufio"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"os/exec"
	"strings"
	"sync"
)

func main() {
	config := NewMemcachedConfiguration()

	workers := config.poolSize

	var wg sync.WaitGroup

	dstClient := NewMemcachedClient(config.dstMemcachedServers, config.timeout, workers, config.dstMemcachedTLS)

	for _, srcServer := range config.srcMemcachedServers {
		wg.Add(1)
		srcClient := NewMemcachedClient([]string{srcServer}, config.timeout, workers, config.srcMemcachedTLS)
		go func() {
			if config.srcMemcachedTLS {
				srcServer = "tls:" + srcServer
			}
			log.Printf("Start migrate data from server: %s to servers: %v\n", srcServer, config.dstMemcachedServers)
			migrate(config.memcachedOfficialToolPath, srcServer, srcClient.client, dstClient.client, &wg, workers)
			wg.Done()
		}()
	}

	wg.Wait()
	log.Println("Migration completed")
}

func migrate(memcachedOfficialTool, server string, mcSrc, mcDst *memcache.Client, wg *sync.WaitGroup, workers int) {
	ch := make(chan string, workers)

	cmd := exec.Command(memcachedOfficialTool, server, "keys")

	// Get the standard output pipe of the command
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("Unable to obtain standard output pipe: %v", err)
		return
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		log.Printf("Failed to start command: %v", err)
		return
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			for key := range ch {
				item, err := mcSrc.Get(key)
				if err != nil {
					log.Printf("Get key %s error: %v", key, err)
					continue
				}
				if item != nil {
					err = mcDst.Set(item)
					if err != nil {
						log.Printf("Set key %s error: %v", key, err)
						continue
					}
				} else {
					log.Printf("Key %s not found", key)
					continue
				}
			}
			wg.Done()
		}()
	}

	// Read the output line by line
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		key := strings.Split(strings.Split(line, " ")[0], "=")[1]
		ch <- key
	}
	close(ch)

	// Check if there was an error reading the output
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading output: %v", err)
	}

	// Wait for the command to finish executing
	if err := cmd.Wait(); err != nil {
		log.Printf("Command failed: %v", err)
	}

}
