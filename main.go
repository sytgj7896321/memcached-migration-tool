package main

import (
	"bufio"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

func main() {
	memcachedOfficialTool := "memcached-tool"
	tool := os.Getenv("MEMCACHED_TOOL_PATH")
	if tool != "" {
		memcachedOfficialTool = tool
	}

	srcServerList := os.Getenv("SRC_MEMCACHED_SERVERS")
	if srcServerList == "" {
		log.Fatalln("SRC_MEMCACHED_SERVERS environment variable not set")
	}
	srcServers := strings.Split(srcServerList, ",")

	destServerList := os.Getenv("DEST_MEMCACHED_SERVERS")
	if destServerList == "" {
		log.Fatalln("DEST_MEMCACHED_SERVERS environment variable not set")
	}
	destServers := strings.Split(destServerList, ",")

	mcDest := memcache.New(destServers...)

	var wg sync.WaitGroup

	for _, srcServer := range srcServers {
		wg.Add(1)
		go func() {
			mcSrc := memcache.New(srcServer)
			log.Printf("Start migrate data from server: %s\n", srcServer)
			migrate(memcachedOfficialTool, srcServer, mcSrc, mcDest)
			defer wg.Done()
		}()
	}

	wg.Wait()
}

func migrate(memcachedOfficialTool, server string, mcSrc, mcDest *memcache.Client) {
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

	// Read the output line by line
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		key := strings.Split(strings.Split(line, " ")[0], "=")[1]
		item, err := mcSrc.Get(key)
		if err != nil {
			log.Printf("Get key %s error: %v", key, err)
			continue
		}
		if item != nil {
			err = mcDest.Set(item)
			if err != nil {
				log.Printf("Set key %s error: %v", key, err)
				continue
			}
		} else {
			log.Printf("Key %s not found", key)
			continue
		}
	}

	// Check if there was an error reading the output
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading output: %v", err)
	}

	// Wait for the command to finish executing
	if err := cmd.Wait(); err != nil {
		log.Printf("Command failed: %v", err)
	}

}
