// Command pebbisd runs a Pebbis instance as a standalone RESP server.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/pebbis/pebbis"
	"github.com/pebbis/pebbis/config"
)

func main() {
	var (
		addr        = flag.String("addr", ":6379", "listen address")
		dir         = flag.String("dir", "", "data directory (empty = in-memory)")
		maxMem      = flag.String("maxmemory", "0", "in-memory index budget, e.g. 256mb (0 = unbounded)")
		policy      = flag.String("maxmemory-policy", "noeviction", "eviction policy")
		mode        = flag.String("eviction-mode", "persist", "persist keeps data on eviction, cache deletes it")
		cache       = flag.Int64("block-cache", 64<<20, "Pebble block cache size in bytes")
		syncMode    = flag.String("appendfsync", "group", "group | always | never")
		databases   = flag.Int("databases", 16, "number of logical databases")
		requirePass = flag.String("requirepass", "", "require clients to issue AUTH with this password")
		maxClients  = flag.Int("maxclients", 0, "cap on concurrent connections (0 = unlimited)")
	)
	flag.Parse()

	cfg := pebbis.DefaultOptions()
	cfg.Dir = *dir
	maxMemory, err := pebbis.ParseByteSize(*maxMem)
	if err != nil {
		log.Fatalf("invalid -maxmemory: %v", err)
	}
	cfg.MaxMemory = maxMemory
	cfg.EvictionPolicy = config.EvictionPolicy(*policy)
	cfg.EvictionMode = config.EvictionMode(*mode)
	cfg.BlockCacheSize = *cache
	cfg.SyncPolicy = config.SyncPolicy(*syncMode)
	cfg.Databases = *databases
	cfg.RequirePass = *requirePass

	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	if cfg.Dir != "" {
		if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
			log.Fatalf("create data dir: %v", err)
		}
		abs, err := filepath.Abs(cfg.Dir)
		if err == nil {
			cfg.Dir = abs
		}
	}

	store, err := pebbis.Open(cfg)
	if err != nil {
		log.Fatalf("open: %v", err)
	}

	srv := pebbis.NewServer(store, pebbis.ServerOptions{
		Addr:       *addr,
		MaxClients: *maxClients,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Pebbis %s listening on %s (dir=%q, commands=%d, maxmemory=%d, policy=%s)",
		pebbis.Version, *addr, cfg.Dir, pebbis.CommandCount(), cfg.MaxMemory, cfg.EvictionPolicy)

	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("server error: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
	case <-store.Shutdown():
		log.Printf("received SHUTDOWN, shutting down")
	}

	// Stop accepting first, then let Pebble close cleanly so the WAL is
	// complete and the next start needs no recovery.
	if err := srv.Close(); err != nil {
		log.Printf("close listener: %v", err)
	}
	if err := store.Close(); err != nil {
		log.Printf("close store: %v", err)
	}
}
