const std = @import("std");
const lib = @import("lib.zig");

const Stmt = lib.Stmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

// The statements a connection prepared automatically (Conn.Opts.statement_cache_size),
// keyed by their SQL, least recently used evicted first. Each one is a named
// prepared statement on the server; reusing it skips the Parse/Describe round
// trip, so the query takes one round trip instead of two.
pub const StatementCache = struct {
    capacity: u16,

    // to name the next statement
    next_id: u64 = 1,

    // Statements evicted (or invalidated) here but still prepared on the
    // server. The next Parse closes them, in the same round trip.
    pending_close: std.ArrayList(u64) = .empty,

    lookup: std.StringHashMapUnmanaged(*Entry) = .empty,

    // most recently used first
    lru: std.DoublyLinkedList = .{},

    hits: u64 = 0,
    misses: u64 = 0,

    pub const name_prefix = "pgz_auto_";
    const max_name_len = name_prefix.len + 20;

    pub const Entry = struct {
        node: std.DoublyLinkedList.Node = .{},
        id: u64,
        // owned by describe.arena
        sql: []const u8,
        describe: Stmt.Describe,
        _name_buf: [max_name_len]u8,
        _name_len: u8,

        pub fn name(self: *const Entry) []const u8 {
            return self._name_buf[0..self._name_len];
        }
    };

    pub const Stats = struct {
        size: usize,
        hits: u64,
        misses: u64,
    };

    pub fn init(capacity: u16) StatementCache {
        return .{ .capacity = capacity };
    }

    pub fn deinit(self: *StatementCache, allocator: Allocator) void {
        while (self.lru.popFirst()) |node| {
            destroy(allocator, @fieldParentPtr("node", node));
        }
        self.lookup.deinit(allocator);
        self.pending_close.deinit(allocator);
    }

    pub fn enabled(self: *const StatementCache) bool {
        return self.capacity > 0;
    }

    // The entry for sql, now the most recently used, or null.
    pub fn get(self: *StatementCache, sql: []const u8) ?*Entry {
        const entry = self.lookup.get(sql) orelse {
            self.misses += 1;
            return null;
        };
        self.hits += 1;
        self.lru.remove(&entry.node);
        self.lru.prepend(&entry.node);
        return entry;
    }

    // A new entry for sql, with a name the server doesn't know yet. It isn't
    // in the cache until `add` is called, once it's prepared. If preparing it
    // fails, `discard` must be called instead.
    pub fn create(self: *StatementCache, allocator: Allocator, sql: []const u8) !*Entry {
        // so that neither add nor discard can fail
        try self.lookup.ensureUnusedCapacity(allocator, 1);
        try self.pending_close.ensureUnusedCapacity(allocator, 1);

        const entry = try allocator.create(Entry);
        errdefer allocator.destroy(entry);

        const id = self.next_id;
        self.next_id = id + 1;

        entry.* = .{
            .id = id,
            .sql = undefined,
            .describe = .{
                .arena = ArenaAllocator.init(allocator),
                .param_oids = &.{},
                .result_state = undefined,
            },
            ._name_buf = undefined,
            ._name_len = @intCast((std.fmt.bufPrint(&entry._name_buf, name_prefix ++ "{d}", .{id}) catch unreachable).len),
        };
        errdefer entry.describe.arena.deinit();
        entry.sql = try entry.describe.arena.allocator().dupe(u8, sql);
        return entry;
    }

    fn destroy(allocator: Allocator, entry: *Entry) void {
        entry.describe.arena.deinit();
        allocator.destroy(entry);
    }

    // Frees an entry from `create` that couldn't be prepared. Its statement
    // may exist on the server all the same (if Parse worked but something
    // after it failed), so the next Parse closes it; closing a statement that
    // doesn't exist isn't an error.
    pub fn discard(self: *StatementCache, allocator: Allocator, entry: *Entry) void {
        self.pending_close.appendAssumeCapacity(entry.id);
        destroy(allocator, entry);
    }

    // Adds an entry from `create`, once prepared. If that makes the cache too
    // big, the least recently used entry is evicted and closed on the server
    // by the next Parse.
    pub fn add(self: *StatementCache, allocator: Allocator, entry: *Entry) void {
        self.lookup.putAssumeCapacity(entry.sql, entry);
        self.lru.prepend(&entry.node);

        if (self.lookup.count() > self.capacity) {
            const last: *Entry = @fieldParentPtr("node", self.lru.last.?);
            self.removeEntry(allocator, last);
        }
    }

    // Drops an entry the server may no longer accept (e.g. after
    // "cached plan must not change result type"), so its SQL is prepared
    // again next time. It's closed on the server by the next Parse.
    pub fn invalidate(self: *StatementCache, allocator: Allocator, entry: *Entry) void {
        self.pending_close.ensureUnusedCapacity(allocator, 1) catch {
            // Can't remember to close it: leave it cached. It'll fail again,
            // which is no worse than not invalidating it.
            return;
        };
        self.removeEntry(allocator, entry);
    }

    fn removeEntry(self: *StatementCache, allocator: Allocator, entry: *Entry) void {
        // capacity was reserved by create or invalidate
        self.pending_close.appendAssumeCapacity(entry.id);
        _ = self.lookup.remove(entry.sql);
        self.lru.remove(&entry.node);
        destroy(allocator, entry);
    }

    pub fn stats(self: *const StatementCache) Stats {
        return .{
            .size = self.lookup.count(),
            .hits = self.hits,
            .misses = self.misses,
        };
    }

    // Writes the name of the statement with this id.
    pub fn writeName(buf: []u8, id: u64) []const u8 {
        return std.fmt.bufPrint(buf, name_prefix ++ "{d}", .{id}) catch unreachable;
    }
};

const t = lib.testing;
test "StatementCache: lru" {
    var cache = StatementCache.init(2);
    defer cache.deinit(t.allocator);

    try t.expectEqual(null, cache.get("q1"));
    const e1 = try cache.create(t.allocator, "q1");
    try t.expectString("pgz_auto_1", e1.name());
    cache.add(t.allocator, e1);

    const e2 = try cache.create(t.allocator, "q2");
    cache.add(t.allocator, e2);
    try t.expectString("pgz_auto_2", e2.name());

    // q1 is now the most recently used, so q2 goes first
    try t.expectEqual(e1, cache.get("q1").?);
    cache.add(t.allocator, try cache.create(t.allocator, "q3"));
    try t.expectEqual(null, cache.get("q2"));
    try t.expectSlice(u64, &.{2}, cache.pending_close.items);
    try t.expectEqual(e1, cache.get("q1").?);

    cache.invalidate(t.allocator, e1);
    try t.expectEqual(null, cache.get("q1"));
    try t.expectSlice(u64, &.{ 2, 1 }, cache.pending_close.items);

    const stats = cache.stats();
    try t.expectEqual(1, stats.size);
    try t.expectEqual(2, stats.hits);
    try t.expectEqual(3, stats.misses);
}

test "StatementCache: discard" {
    var cache = StatementCache.init(2);
    defer cache.deinit(t.allocator);
    const e = try cache.create(t.allocator, "q1");
    cache.discard(t.allocator, e);
    try t.expectEqual(0, cache.stats().size);
    try t.expectSlice(u64, &.{1}, cache.pending_close.items);
}
