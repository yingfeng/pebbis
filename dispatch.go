package redistore

import "sort"

// command is one entry in the dispatch table.
type command struct {
	fn func(*Ctx, [][]byte) error
	// arity follows Redis' convention: positive is exact, negative means
	// "at least |arity|", and it counts the command name itself.
	arity int
	// write marks commands that must be rejected on a read-only replica.
	// Reserved for the replication work in P3.
	write bool
}

// commands is the dispatch table. Command names are upper-case; lookup folds
// the incoming name before matching.
var commands = map[string]command{
	// Connection.
	"PING":    {cmdPing, -1, false},
	"ECHO":    {cmdEcho, 2, false},
	"SELECT":  {cmdSelect, 2, false},
	"QUIT":    {cmdQuit, 1, false},
	"COMMAND": {cmdCommand, -1, false},
	"HELLO":   {cmdHello, -1, false},

	// Strings.
	"GET":      {cmdGet, 2, false},
	"SET":      {cmdSet, -3, true},
	"SETNX":    {cmdSetNX, 3, true},
	"SETXX":    {cmdSetXX, 3, true},
	"SETEX":    {cmdSetEx, 4, true},
	"PSETEX":   {cmdPSetEx, 4, true},
	"GETSET":   {cmdGetSet, 3, true},
	"GETDEL":   {cmdGetDel, 2, true},
	"MSET":     {cmdMSet, -3, true},
	"MSETNX":   {cmdMSetNX, -3, true},
	"MGET":     {cmdMGet, -2, false},
	"APPEND":   {cmdAppend, 3, true},
	"STRLEN":   {cmdStrLen, 2, false},
	"INCR":     {cmdIncr, 2, true},
	"DECR":     {cmdDecr, 2, true},
	"INCRBY":   {cmdIncrBy, 3, true},
	"DECRBY":   {cmdDecrBy, 3, true},
	"GETRANGE": {cmdGetRange, 4, false},
	"SETRANGE": {cmdSetRange, 4, true},
	"SUBSTR":   {cmdGetRange, 4, false},

	// Generic.
	"DEL":       {cmdDel, -2, true},
	"UNLINK":    {cmdUnlink, -2, true},
	"EXISTS":    {cmdExists, -2, false},
	"TYPE":      {cmdType, 2, false},
	"TTL":       {cmdTTL, 2, false},
	"PTTL":      {cmdPTTL, 2, false},
	"EXPIRE":    {cmdExpire, -3, true},
	"PEXPIRE":   {cmdPExpire, -3, true},
	"EXPIREAT":  {cmdExpireAt, -3, true},
	"PEXPIREAT": {cmdPExpireAt, -3, true},
	"PERSIST":   {cmdPersist, 2, true},
	"KEYS":      {cmdKeys, 2, false},
	"DBSIZE":    {cmdDBSize, 1, false},
	"FLUSHDB":   {cmdFlushDB, -1, true},
	"FLUSHALL":  {cmdFlushAll, -1, true},
	"RENAME":    {cmdRename, 3, true},
	"RENAMENX":  {cmdRenameNX, 3, true},
	"SCAN":      {cmdScan, -2, false},
	"TOUCH":     {cmdTouch, -2, false},

	// Hashes.
	"HSET":       {cmdHSet, -4, true},
	"HSETNX":     {cmdHSetNX, 4, true},
	"HGET":       {cmdHGet, 3, false},
	"HDEL":       {cmdHDel, -3, true},
	"HEXISTS":    {cmdHExists, 3, false},
	"HLEN":       {cmdHLen, 2, false},
	"HKEYS":      {cmdHKeys, 2, false},
	"HVALS":      {cmdHVals, 2, false},
	"HGETALL":    {cmdHGetAll, 2, false},
	"HMGET":      {cmdHMGet, -3, false},
	"HMSET":      {cmdHMSet, -4, true},
	"HINCRBY":    {cmdHIncrBy, 4, true},
	"HSTRLEN":    {cmdHStrLen, 3, false},
	"HRANDFIELD": {cmdHRandField, -2, false},

	// Lists.
	"LPUSH":  {cmdLPush, -3, true},
	"RPUSH":  {cmdRPush, -3, true},
	"LPUSHX": {cmdLPushX, -3, true},
	"RPUSHX": {cmdRPushX, -3, true},
	"LPOP":   {cmdLPop, -2, true},
	"RPOP":   {cmdRPop, -2, true},
	"LRANGE": {cmdLRange, 4, false},
	"LLEN":   {cmdLLen, 2, false},
	"LINDEX": {cmdLIndex, 3, false},
	"LSET":   {cmdLSet, 4, true},
	"LREM":   {cmdLRem, 4, true},
	"LTRIM":  {cmdLTrim, 4, true},
	// Blocking variants. These hold their connection's goroutine, which is
	// fine: each connection has one, and nothing else waits on it.
	"BLPOP":      {cmdBLPop, -3, true},
	"BRPOP":      {cmdBRPop, -3, true},
	"BRPOPLPUSH": {cmdBRPopLPush, 4, true},
	"RPOPLPUSH":  {cmdRPopLPush, 3, true},

	// Sets.
	"SADD":        {cmdSAdd, -3, true},
	"SREM":        {cmdSRem, -3, true},
	"SMEMBERS":    {cmdSMembers, 2, false},
	"SISMEMBER":   {cmdSIsMember, 3, false},
	"SMISMEMBER":  {cmdSMIsMember, -3, false},
	"SCARD":       {cmdSCard, 2, false},
	"SMOVE":       {cmdSMove, 4, true},
	"SPOP":        {cmdSPop, -2, true},
	"SRANDMEMBER": {cmdSRandMember, -2, false},
	"SUNION":      {cmdSUnion, -2, false},
	"SINTER":      {cmdSInter, -2, false},
	"SDIFF":       {cmdSDiff, -2, false},
	"SUNIONSTORE": {cmdSUnionStore, -3, true},
	"SINTERSTORE": {cmdSInterStore, -3, true},
	"SDIFFSTORE":  {cmdSDiffStore, -3, true},
	"SINTERCARD":  {cmdSInterCard, -2, false},

	// Sorted sets.
	"ZADD":             {cmdZAdd, -4, true},
	"ZREM":             {cmdZRem, -3, true},
	"ZSCORE":           {cmdZScore, 3, false},
	"ZMSCORE":          {cmdZMScore, -3, false},
	"ZCARD":            {cmdZCard, 2, false},
	"ZRANK":            {cmdZRank, 3, false},
	"ZREVRANK":         {cmdZRevRank, 3, false},
	"ZINCRBY":          {cmdZIncrBy, 4, true},
	"ZRANGE":           {cmdZRange, -4, false},
	"ZRANGEBYSCORE":    {cmdZRangeByScore, -4, false},
	"ZCOUNT":           {cmdZCount, 4, false},
	"ZPOPMIN":          {cmdZPopMin, -2, true},
	"ZPOPMAX":          {cmdZPopMax, -2, true},
	"ZREMRANGEBYRANK":  {cmdZRemRangeByRank, 4, true},
	"ZREMRANGEBYSCORE": {cmdZRemRangeByScore, 4, true},

	// Transactions.
	"MULTI":   {cmdMulti, 1, false},
	"EXEC":    {cmdExec, 1, false},
	"DISCARD": {cmdDiscard, 1, false},
	"WATCH":   {cmdWatch, -2, false},
	"UNWATCH": {cmdUnwatch, 1, false},

	// Pub/Sub.
	"SUBSCRIBE":    {cmdSubscribe, -2, false},
	"PSUBSCRIBE":   {cmdPSubscribe, -2, false},
	"UNSUBSCRIBE":  {cmdUnsubscribe, -1, false},
	"PUNSUBSCRIBE": {cmdPUnsubscribe, -1, false},
	"PUBLISH":      {cmdPublish, 3, false},
	"PUBSUB":       {cmdPubSub, -2, false},

	// Server.
	"AUTH":     {cmdAuth, -2, false},
	"INFO":     {cmdInfo, -1, false},
	"CONFIG":   {cmdConfig, -2, false},
	"SAVE":     {cmdSave, 1, false},
	"BGSAVE":   {cmdSave, 1, false},
	"LASTSAVE": {cmdLastSave, 1, false},
	"TIME":     {cmdTime, 1, false},
	"MEMORY":   {cmdMemory, -2, false},
	"SLOWLOG":  {cmdSlowLog, -2, false},
	"CLIENT":   {cmdClient, -2, false},
	"OBJECT":   {cmdObject, -3, false},
	"SHUTDOWN": {cmdShutdown, -1, false},
}

// commandCount and commandNames are precomputed in init.
//
// They cannot be derived lazily: commands references cmdCommand, which reports
// COMMAND COUNT, and a lazy lookup would make that an initialisation cycle.
var (
	commandCount int
	commandNames []string
)

func init() {
	commandCount = len(commands)
	commandNames = make([]string, 0, len(commands))
	for n := range commands {
		commandNames = append(commandNames, n)
	}
	sort.Strings(commandNames)
	// Bound here rather than in lookupCommand: EXEC dispatches queued commands
	// and would otherwise make commands depend on itself.
	lookupFn = func(name string) (command, bool) {
		c, ok := commands[name]
		return c, ok
	}
}

// lookupFn is the resolved dispatcher, set in init.
var lookupFn func(string) (command, bool)

// lookupCommand resolves a command name.
func lookupCommand(name string) (command, bool) {
	return lookupFn(name)
}

// CommandCount returns the number of supported commands.
func CommandCount() int { return commandCount }

// CommandNames returns the sorted list of supported command names.
func CommandNames() []string { return commandNames }
