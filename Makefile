BINARY  := redistored
GO      ?= go
PKGS    := ./...

.PHONY: all build test race bench cover fmt vet clean

all: build

build:
	$(GO) build -o bin/$(BINARY) ./cmd/$(BINARY)

test:
	$(GO) test -count=1 $(PKGS)

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
