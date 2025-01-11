package main

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
)

func main() {
	config := NewMemcachedConfiguration()

	workers := config.poolSize

	var wg sync.WaitGroup

	for _, srcServer := range config.srcMemcachedServers {
		wg.Add(1)
		go func() {
			log.Printf("Start migrate data from server: %s to servers: %v", srcServer, config.dstMemcachedServers)
			migrate(srcServer, config, &wg, workers)
			wg.Done()
		}()
	}

	wg.Wait()
	log.Println("Migration completed")
}

func migrate(server string, config MemcachedConfig, wg *sync.WaitGroup, workers int) {
	ch := make(chan string, workers)

	src := NewMemcachedClient([]string{server}, config.timeout, workers, config.srcMemcachedTLS)
	err := src.client.Ping()
	if err != nil {
		log.Printf("Ping server %s error: %v", server, err)
		return
	}
	dst := NewMemcachedClient(config.dstMemcachedServers, config.timeout, workers, config.dstMemcachedTLS)
	err = dst.client.Ping()
	if err != nil {
		log.Printf("Ping servers %v error: %v", config.dstMemcachedServers, err)
		return
	}

	defer func(client *memcache.Client) {
		err := client.Close()
		if err != nil {
			log.Printf("Error closing source client: %v", err)
		}
	}(dst.client)
	defer func(client *memcache.Client) {
		err := client.Close()
		if err != nil {
			log.Printf("Error closing destination client: %v", err)
		}
	}(src.client)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range ch {
				item, err := src.client.Get(key)
				if err != nil {
					log.Printf("Get key %s error: %v", key, err)
					continue
				}
				if item == nil {
					log.Printf("Key %s not found", key)
					continue
				}
				err = dst.client.Set(item)
				if err != nil {
					log.Printf("Set key %s error: %v", item.Key, err)
					continue
				}
			}
		}()
	}

	listKeys(server, ch, config.srcMemcachedTLS)
}

func listKeys(server string, ch chan string, enableTls bool) {
	var conn net.Conn
	var err error
	if enableTls {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.Dial("tcp", server, tlsConfig)
	} else {
		conn, err = net.Dial("tcp", server)
	}
	if err != nil {
		log.Printf("Connect to server error: %v", err)
		return
	}

	_, err = conn.Write([]byte(fmt.Sprintf("lru_crawler metadump all\r\n")))
	if err != nil {
		log.Printf("List keys error: %v", err)
		return
	}

	reader := bufio.NewReader(conn)
	for {
		response, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Read keys error: %v", err)
			continue
		}

		response = strings.TrimSpace(response)
		if response == "END" {
			close(ch)
			break
		}

		parts := strings.Split(response, " ")
		encodedKey := strings.TrimPrefix(parts[0], "key=")
		decodedKey, err := url.QueryUnescape(encodedKey)
		if err != nil {
			log.Printf("Decode key error: %v", err)
			continue
		}
		ch <- decodedKey
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)
}
