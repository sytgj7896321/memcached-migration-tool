package main

import (
	"context"
	"crypto/tls"
	"github.com/bradfitz/gomemcache/memcache"
	"net"
	"time"
)

type MemcachedClient struct {
	client *memcache.Client
}

func NewMemcachedClient(servers []string, timeout time.Duration, workers int, enableTLS bool) *MemcachedClient {
	c := memcache.New(servers...)
	c.Timeout = timeout
	c.MaxIdleConns = workers
	if enableTLS {
		c.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			var td tls.Dialer
			td.Config = &tls.Config{
				InsecureSkipVerify: true,
			}
			return td.DialContext(ctx, network, addr)
		}
	}
	return &MemcachedClient{
		client: c,
	}
}
