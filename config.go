package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type MemcachedConfig struct {
	memcachedOfficialToolPath string
	poolSize                  int
	timeout                   time.Duration
	srcMemcachedTLS           bool
	dstMemcachedTLS           bool
	srcMemcachedServers       []string
	dstMemcachedServers       []string
}

func NewMemcachedConfiguration() MemcachedConfig {
	config := MemcachedConfig{}
	config.memcachedOfficialToolPath = "memcached-tool"
	tool := os.Getenv("MEMCACHED_TOOL_PATH")
	if tool != "" {
		config.memcachedOfficialToolPath = tool
	}

	config.poolSize = 200
	poolSizeEnv := os.Getenv("MEMCACHED_POOL_SIZE")
	if poolSizeEnv != "" {
		p, err := strconv.Atoi(poolSizeEnv)
		if err != nil {
			log.Fatalf("Invalid MEMCACHED_POOL_SIZE: %v\n", err)
		}
		config.poolSize = p
	}

	config.timeout = 10 * time.Second
	timeoutEnv := os.Getenv("MEMCACHED_TIMEOUT")
	if timeoutEnv != "" {
		t, err := time.ParseDuration(timeoutEnv)
		if err != nil {
			log.Fatalf("Invalid MEMCACHED_TIMEOUT: %v\n", err)
		}
		config.timeout = t
	}

	config.srcMemcachedTLS = false
	srcTLSEnv := os.Getenv("SRC_MEMCACHED_TLS")
	if strings.ToUpper(srcTLSEnv) == "TRUE" {
		config.srcMemcachedTLS = true
	}

	config.dstMemcachedTLS = false
	dstTLSEnv := os.Getenv("DST_MEMCACHED_TLS")
	if strings.ToUpper(dstTLSEnv) == "TRUE" {
		config.dstMemcachedTLS = true
	}

	srcServerList := os.Getenv("SRC_MEMCACHED_SERVERS")
	if srcServerList == "" {
		log.Fatalln("SRC_MEMCACHED_SERVERS environment variable not set")
	}
	config.srcMemcachedServers = strings.Split(srcServerList, ",")

	dstServerList := os.Getenv("DST_MEMCACHED_SERVERS")
	if dstServerList == "" {
		log.Fatalln("DST_MEMCACHED_SERVERS environment variable not set")
	}
	config.dstMemcachedServers = strings.Split(dstServerList, ",")

	return config
}
