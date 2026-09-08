# redistore

An embedded, Redis-compatible data store written in Go, with [Pebble](https://github.com/cockroachdb/pebble) as its storage engine.

It can be used two ways:

- **As a library** — import it, open a `Store`, and talk RESP semantics without any socket. Persistence, expiration, eviction and transactions all work in-process.
- **As a server** — `cmd/redistored` speaks RESP2 over TCP, so any Redis client (`redis-cli`, go-redis, …) can connect.

## Design highlights

- **Pebble-backed durability.** Every key lives in an LSM tree; restarts replay nothing but Pebble's own WAL. `Checkpoint` provides consistent hard-link snapshots.
- **Skiplist-free in-memory index.** Aggregates (hash/list/set/zset/stream) are reconstructed lazily from the engine, bounded by cache size — memory stays proportional to the working set, not the dataset.
- **Eviction + TTL.** `maxmemory` with `noeviction` / `allkeys-lru` / `volatile-lru` / `allkeys-lfu` / `volatile-lfu` / `allkeys-random` / `volatile-random` / `volatile-ttl`, plus lazy + active key expiration.
- **Atomic batches.** `MULTI`/`EXEC` map onto Pebble batch commits; a failed command aborts the whole queue atomically.
- **Reliable streams.** Consumer groups with PEL: deliveries are recorded before they are sent, and acked entries are removed — pending entries survive restarts.

## Quick start

Embedded:

```go
store, err := redistore.Open(redistore.WithDir("/path/to/data"))
if err != nil {
    return err
}
defer store.Close()
```

Server:

```
go build -o bin/redistored ./cmd/redistored
./bin/redistored -addr :6379 -dir ./data
```

Configuration (see `config/`): `maxmemory`, `maxmemory-policy`, `slowlog-log-slower-than`, `slowlog-max-len`, `lfu-decay-time`.

## Supported commands (188)

- **Connection / server**: `PING`, `ECHO`, `SELECT`, `QUIT`, `HELLO`, `AUTH`, `COMMAND`, `CLIENT` (GET/GETNAME/ID/INFO/LIST/RESET/SETNAME), `CONFIG` (subset), `INFO`, `DEBUG`, `TIME`, `SORT`, `SORT_RO`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SAVE`, `BGSAVE`, `LASTSAVE`, `SHUTDOWN`, `SLOWLOG`, `MEMORY`, `OBJECT` (subset)
- **Strings**: `GET`, `SET`, `SETNX`, `SETXX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `DELEX`, `MSET`, `MSETNX`, `MGET`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `GETRANGE`, `SETRANGE`, `SUBSTR`, `LCS`, `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`
- **Keys (generic)**: `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `EXPIRETIME`, `PEXPIRETIME`, `KEYS`, `SCAN`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `COPY`, `MOVE`, `SWAPDB`, `TOUCH`
- **Hashes**: `HSET`, `HMSET`, `HSETNX`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`, `HSTRLEN`, `HINCRBY`, `HINCRBYFLOAT`, `HSCAN`, `HRANDFIELD`
- **Lists**: `LPUSH`, `LPUSHX`, `RPUSH`, `RPUSHX`, `LPOP`, `RPOP`, `LRANGE`, `LINDEX`, `LLEN`, `LREM`, `LSET`, `LTRIM`, `LINSERT`, `LMOVE`, `LPOS`, `LMPOP`, `RPOPLPUSH`, `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BLMOVE`, `BLMPOP`
- **Sets**: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SINTER`, `SINTERSTORE`, `SINTERCARD`, `SUNION`, `SUNIONSTORE`, `SDIFF`, `SDIFFSTORE`, `SSCAN`
- **Sorted sets**: `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`, `ZREVRANGE`, `ZRANGE` (incl. `REV`/`BYSCORE`/`BYLEX`), `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZRANGESTORE`, `ZLEXCOUNT`, `ZREM`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `ZRANDMEMBER`, `ZUNION`, `ZUNIONSTORE`, `ZINTER`, `ZINTERSTORE`, `ZDIFF`, `ZDIFFSTORE`
- **Streams**: `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XDEL`, `XTRIM`, `XGROUP` (CREATE/DESTROY/CREATECONSUMER/DELCONSUMER), `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XINFO` (GROUPS/CONSUMERS/STREAM)
- **Pub/Sub**: `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB`
- **Transactions**: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

## Unsupported commands

This is a cache/store, not a full Redis clone. Compared with Redis 7.x, the following are **not implemented**:

- **Clustering & replication**: `CLUSTER *`, `REPLICAOF`, `SLAVEOF`, `SYNC`, `PSYNC`, `WAIT`, `WAITAOF`, `FAILOVER`, `READONLY`, `READWRITE`
- **ACL**: the entire `ACL *` family (authentication via `AUTH <password>` works with a configured default password)
- **Bitmaps**: `BITFIELD`, `BITFIELD_RO` (the `SETBIT` / `GETBIT` / `BITCOUNT` / `BITPOS` / `BITOP` primitives are supported)
- **HyperLogLog**: `PFADD`, `PFCOUNT`, `PFMERGE`
- **Geospatial**: `GEOADD` and the whole `GEO*` family
- **Scripting / functions**: `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `FCALL`, `FCALL_RO`, `SCRIPT *`, `FUNCTION *`
- **Server internals**: `MONITOR`, `LATENCY *`, `LOLWUT`, `COMMAND DOCS/INFO` (only the full `COMMAND` dump is served)
- **Data-command gaps**: `DUMP`, `RESTORE`, `MIGRATE`
- **Hash field TTL (Redis 7.4)**: `HGETEX`, `HGETDEL`, `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST`, `HFIELDS`
- **Streams**: `XAUTOCLAIM`, `XSETID`
- **Keyspace notifications**: expired-key / keyevent channels are not published

Anything not listed here will return an `ERR unknown command` error, same as Redis.

## Development

```
make build   # build cmd/redistored into bin/
make race    # go test -race across all packages (the standard gate)
make bench   # run benchmarks (XADD, sparse aggregates, ...)
make cover   # coverage profile + HTML report
```

Layout: `storage/` (Pebble engine wrapper), `memory/` (aggregate encodings), `stream.go` (streams + consumer groups), `cmd_*.go` (one file per data type), `tests/compat/` (black-box RESP compatibility tests).
