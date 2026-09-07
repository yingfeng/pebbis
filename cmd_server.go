package redistore

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Server introspection commands.

// Version is reported through INFO.
const Version = "0.1.0"

func cmdInfo(c *Ctx, args [][]byte) error {
	if len(args) > 1 {
		return ErrSyntax
	}
	section := "default"
	if len(args) == 1 {
		section = strings.ToLower(string(args[0]))
	}

	s := c.Store
	var b strings.Builder

	if section == "default" || section == "all" || section == "server" {
		b.WriteString("# Server\r\n")
		fmt.Fprintf(&b, "redistore_version:%s\r\n", Version)
		fmt.Fprintf(&b, "redis_mode:standalone\r\n")
		fmt.Fprintf(&b, "databases:%d\r\n", s.DBCount())
		fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int64(s.Uptime().Seconds()))
		b.WriteString("\r\n")
	}
	if section == "default" || section == "all" || section == "clients" {
		b.WriteString("# Clients\r\n")
		b.WriteString("connected_clients:0\r\n") // filled in by the server layer
		b.WriteString("\r\n")
	}
	if section == "default" || section == "all" || section == "memory" {
		b.WriteString("# Memory\r\n")
		fmt.Fprintf(&b, "used_memory:%d\r\n", s.UsedMemory())
		fmt.Fprintf(&b, "used_memory_human:%s\r\n", humanBytes(uint64(s.UsedMemory())))
		fmt.Fprintf(&b, "maxmemory:%d\r\n", s.cfg.MaxMemory)
		fmt.Fprintf(&b, "maxmemory_human:%s\r\n", humanBytes(s.cfg.MaxMemory))
		fmt.Fprintf(&b, "maxmemory_policy:%s\r\n", s.cfg.EvictionPolicy)
		fmt.Fprintf(&b, "block_cache_memory:%d\r\n", s.BlockCacheMemory())
		b.WriteString("\r\n")
	}
	if section == "default" || section == "all" || section == "persistence" {
		b.WriteString("# Persistence\r\n")
		fmt.Fprintf(&b, "storage_engine:pebble\r\n")
		fmt.Fprintf(&b, "disk_usage:%d\r\n", s.DiskUsage())
		fmt.Fprintf(&b, "rdb_last_save_time:%d\r\n", s.lastSave.Load())
		b.WriteString("\r\n")
	}
	if section == "default" || section == "all" || section == "stats" {
		b.WriteString("# Stats\r\n")
		fmt.Fprintf(&b, "keyspace_hits:%d\r\n", s.stats.hits.Load())
		fmt.Fprintf(&b, "keyspace_misses:%d\r\n", s.stats.misses.Load())
		fmt.Fprintf(&b, "expired_keys:%d\r\n", s.stats.expired.Load()+s.expirer.Expired())
		fmt.Fprintf(&b, "evicted_keys:%d\r\n", s.evictor.Evicted())
		fmt.Fprintf(&b, "total_commands_processed:%d\r\n", s.stats.commands.Load())
		b.WriteString("\r\n")
	}
	if section == "default" || section == "all" || section == "keyspace" {
		b.WriteString("# Keyspace\r\n")
		for db := range s.DBCount() {
			keys := s.dict.Len(uint16(db))
			if keys == 0 {
				continue
			}
			fmt.Fprintf(&b, "db%d:keys=%d,expires=0,avg_ttl=0\r\n", db, keys)
		}
		b.WriteString("\r\n")
	}

	c.w.WriteBulkString(b.String())
	return nil
}

func cmdConfig(c *Ctx, args [][]byte) error {
	if len(args) == 0 {
		return WrongArgs("config")
	}
	switch strings.ToUpper(string(args[0])) {
	case "GET":
		if len(args) < 2 {
			return WrongArgs("config")
		}
		s := c.Store
		// Redis returns a flat field/value array.
		out := make([]string, 0, len(args)*2)
		for _, a := range args[1:] {
			name := strings.ToLower(string(a))
			var val string
			switch name {
			case "maxmemory":
				val = strconv.FormatUint(s.cfg.MaxMemory, 10)
			case "maxmemory-policy":
				val = string(s.cfg.EvictionPolicy)
			case "maxmemory-samples":
				val = strconv.Itoa(s.cfg.EvictionSample)
			case "databases":
				val = strconv.Itoa(s.DBCount())
			case "dir":
				val = s.cfg.Dir
			case "appendfsync":
				val = string(s.cfg.SyncPolicy)
			default:
				continue
			}
			out = append(out, name, val)
		}
		c.w.WriteArray(len(out))
		for _, v := range out {
			c.w.WriteBulkString(v)
		}
		return nil
	case "SET":
		// Dynamic reconfiguration is deliberately narrow: only the knobs that
		// are safe to change at runtime.
		if len(args) != 3 {
			return WrongArgs("config")
		}
		return cmdConfigSet(c, string(args[1]), string(args[2]))
	default:
		return ErrSyntax
	}
}

func cmdSave(c *Ctx, args [][]byte) error {
	if err := c.Store.Flush(); err != nil {
		return err
	}
	c.Store.lastSave.Store(time.Now().Unix())
	c.writeOK()
	return nil
}

func cmdLastSave(c *Ctx, args [][]byte) error {
	c.writeInt(c.Store.lastSave.Load())
	return nil
}

func cmdTime(c *Ctx, args [][]byte) error {
	now := time.Now()
	c.w.WriteArray(2)
	c.w.WriteBulkString(strconv.FormatInt(now.Unix(), 10))
	c.w.WriteBulkString(strconv.FormatInt(now.UnixMicro()%1_000_000, 10))
	return nil
}

func cmdMemory(c *Ctx, args [][]byte) error {
	if len(args) == 0 {
		return WrongArgs("memory")
	}
	switch strings.ToUpper(string(args[0])) {
	case "USAGE":
		if len(args) != 2 {
			return WrongArgs("memory")
		}
		key := string(args[1])
		if e, ok := c.Store.dict.Lookup(c.DB, key); ok {
			c.writeInt(int64(e.Size()))
			return nil
		}
		c.writeNull()
		return nil
	case "STATS":
		c.w.WriteArray(2)
		c.w.WriteBulkString("peak.allocated")
		c.writeInt(c.Store.UsedMemory())
		return nil
	default:
		return ErrSyntax
	}
}

// humanBytes renders a byte count the way INFO does.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := uint64(unit), 0
	for i := n / unit; i >= unit && exp < 5; i /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%cB", float64(n)/float64(div), "KMGTP"[exp])
}
