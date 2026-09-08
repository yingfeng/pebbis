BINARY  := redistored
GO      ?= go
PKGS    := ./...
REDISTORED ?= $(CURDIR)/bin/$(BINARY)

.PHONY: all build test integration race bench cover fmt vet clean

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
