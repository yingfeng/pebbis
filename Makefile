BINARY  := redistored
GO      ?= go
PKGS    := ./...
REDISTORED ?= $(CURDIR)/bin/$(BINARY)

# Standalone concurrency soak test (tests/conctest, run against a real redistored
# via the go-redis client). The suite spins up many goroutines that hammer a
# single key with HSET/SADD/ZADD plus full HSCAN/SSCAN/ZSCAN walks and asserts
# every client observes the expected (stable) totals - i.e. a sparse aggregate is
# never observed half-written. Lives inside the redistore module so the repo is
# self-contained (the go-redis client is a normal dependency via go.mod).
SOAK_PORT   := 6380
SOAK_ADDR   := localhost:$(SOAK_PORT)
SOAK_DIR    := /tmp/redistore-soak
SOAK_PID    := $(SOAK_DIR)/redistored.pid
CONCTEST_DIR ?= tests/conctest

.PHONY: all build test integration race bench cover fmt vet clean soak soak.stop

all: build

build:
	$(GO) build -o bin/$(BINARY) ./cmd/$(BINARY)

# Unit tests for the whole module. The integration suite lives under
# tests/miniredis/integration and skips itself unless INT=1 is set, so this
# target stays green without an external redis.
test:
	$(GO) test -count=1 $(PKGS)

# Differential integration suite: runs miniredis' test harness against redistored,
# asserting the two behave identically. Requires the redistored binary.
integration: build
	INT=1 REDISTORED=$(REDISTORED) $(GO) test -count=1 ./tests/miniredis/integration/

# Full suite with the race detector; the default gate for every change.
race:
	$(GO) test -race -count=1 $(PKGS)

bench:
	$(GO) test -run '^$$' -bench . -benchmem ./...

cover:
	$(GO) test -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -1
	$(GO) tool cover -html=coverage.out -o coverage.html

fmt:
	$(GO) fmt $(PKGS)
	$(GO) vet $(PKGS)

vet:
	$(GO) vet $(PKGS)

clean:
	rm -rf bin coverage.out coverage.html

# soak starts a fresh redistored, runs the concurrent soak suite from
# go-redis/conctest against it, and shuts the server down afterwards.
soak: build
	@mkdir -p $(SOAK_DIR)
	@$(REDISTORED) -addr $(SOAK_ADDR) -dir $(SOAK_DIR)/data > $(SOAK_DIR)/server.log 2>&1 & echo $$! > $(SOAK_PID)
	@echo "waiting for redistored on $(SOAK_ADDR)..."
	@for i in $$(seq 1 50); do \
	  if printf 'PING\r\n' | timeout 1 nc -q1 localhost $(SOAK_PORT) 2>/dev/null | grep -q PONG; then \
	    echo "redistored ready"; break; \
	  fi; \
	  sleep 0.2; \
	done
	@echo "running concurrent soak test..."
	@cd $(CONCTEST_DIR) && REDIS_ADDR=$(SOAK_ADDR) $(GO) test -race -count=1 -timeout 300s ./...; \
	  rc=$$?; \
	  if [ -f $(SOAK_PID) ]; then kill `cat $(SOAK_PID)` 2>/dev/null; rm -f $(SOAK_PID); fi; \
	  exit $$rc

# soak.stop kills a stray redistored left over from a failed soak run.
soak.stop:
	@if [ -f $(SOAK_PID) ]; then kill `cat $(SOAK_PID)` 2>/dev/null; rm -f $(SOAK_PID); fi
	@pkill -f '$(REDISTORED) -addr $(SOAK_ADDR)' 2>/dev/null || true
