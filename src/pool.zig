const std = @import("std");
const lib = @import("lib.zig");

const log = lib.log;
const Conn = lib.Conn;
const Result = lib.Result;
const SSLCtx = lib.SSLCtx;
const QueryRow = lib.QueryRow;
const QueryRowUnsafe = lib.QueryRowUnsafe;
const Listener = @import("listener.zig").Listener;

const Io = std.Io;
const Thread = std.Thread;
const Allocator = std.mem.Allocator;

pub const Pool = struct {
    _opts: Opts,
    _timeout: Io.Duration,
    _conns: []*Conn,
    _available: usize,
    _missing: usize,
    _allocator: Allocator,
    _io: Io,
    _mutex: Io.Mutex,
    _event: Io.Event,
    _ssl_ctx: ?*lib.SSLCtx,
    _reconnector: Reconnector,
    _arena: std.heap.ArenaAllocator,

    pub const Opts = struct {
        size: u16 = 10,
        auth: Conn.AuthOpts = .{},
        connect: Conn.Opts = .{},
        timeout: Io.Duration = .fromSeconds(10),
        connect_on_init_count: ?u16 = null,
    };

    pub const Stats = struct {
        size: usize,
        available: usize,
        missing: usize,
        in_use: usize,
    };

    pub fn initUri(allocator: Allocator, io: Io, uri: std.Uri, opts: Opts) !*Pool {
        var po = try lib.parseOpts(uri, allocator);
        defer po.deinit();
        po.opts.size = opts.size;
        po.opts.timeout = opts.timeout;
        return Pool.init(allocator, io, po.opts);
    }

    pub fn init(allocator: Allocator, io: Io, opts: Opts) !*Pool {
        var arena = std.heap.ArenaAllocator.init(allocator);
        const aa = arena.allocator();
        errdefer arena.deinit();

        const pool = try aa.create(Pool);
        const size = opts.size;
        const conns = try aa.alloc(*Conn, size);

        var opts_copy = opts;
        var ssl_ctx: ?*SSLCtx = null;
        if (comptime lib.has_openssl) {
            switch (opts.connect.tls) {
                .off => {},
                else => |tls_config| {
                    if (opts.connect.host) |h| {
                        opts_copy.connect._hostz = try aa.dupeZ(u8, h);
                    }
                    ssl_ctx = try lib.initializeSSLContext(tls_config);
                },
            }
        }
        errdefer lib.freeSSLContext(ssl_ctx);
        const connect_on_init_count = opts.connect_on_init_count orelse size;

        pool.* = .{
            ._event = .unset,
            ._io = io,
            ._mutex = .init,
            ._conns = conns,
            ._arena = arena,
            ._opts = opts_copy,
            ._ssl_ctx = ssl_ctx,
            ._missing = 0,
            ._allocator = allocator,
            ._available = connect_on_init_count,
            ._reconnector = Reconnector.init(pool),
            ._timeout = opts.timeout,
            // ._timeout = @as(u64, @intCast(opts.timeout)) * std.time.ns_per_ms,
        };

        var opened_connections: usize = 0;
        errdefer {
            for (0..opened_connections) |i| {
                conns[i].deinit();
            }
        }

        for (0..connect_on_init_count) |i| {
            conns[i] = try newConnection(pool, true);
            opened_connections += 1;
        }

        const lazy_start_count = size - connect_on_init_count;
        pool._missing = lazy_start_count;
        for (0..lazy_start_count) |_| {
            try pool._reconnector.reconnect();
        }

        return pool;
    }

    pub fn deinit(self: *Pool) void {
        self._reconnector.stop();
        const allocator = self._allocator;
        for (self._conns) |conn| {
            conn.deinit();
            allocator.destroy(conn);
        }
        lib.freeSSLContext(self._ssl_ctx);
        self._arena.deinit();
    }

    pub fn acquire(self: *Pool) !*Conn {
        const deadline = Io.Clock.awake.now(self._io).addDuration(self._timeout).toNanoseconds();

        while (true) {
            self._mutex.lockUncancelable(self._io);
            const missing = self._missing;
            var available = self._available;
            var conns = self._conns;
            self._mutex.unlock(self._io);

            if (available == 0) {
                // Check if pool is completely exhausted
                const total_alive = conns.len - missing;
                if (total_alive == 0) {
                    return error.PoolExhausted;
                }

                lib.metrics.poolEmpty();

                // Calculate remaining timeout
                const now = Io.Clock.awake.now(self._io).toNanoseconds();
                if (now >= deadline) {
                    std.log.debug("Timeout {} > {}", .{ now, deadline });
                    return error.Timeout;
                }
                const remaining_ns: u64 = @intCast(deadline - now);

                try self._event.waitTimeout(self._io, .{
                    .duration = .{
                        .raw = .fromNanoseconds(remaining_ns),
                        .clock = .awake,
                    },
                });
                self._event.reset();
                continue;
            }

            self._mutex.lockUncancelable(self._io);
            conns = self._conns;
            available = self._available;
            const index = available - 1;
            const conn = conns[index];
            self._available = index;
            defer self._mutex.unlock(self._io);
            return conn;
        }
    }

    pub fn release(self: *Pool, conn: *Conn) void {
        var conn_to_add = conn;

        if (conn._state != .idle) {
            lib.metrics.poolDirty();
            // conn should always be idle when being released. It's possible we can
            // recover from this (e.g. maybe we just need to read until we get a
            // ReadyForQuery), but we wouldn't want to block for too long. For now,
            // we'll just replace the connection.
            conn.deinit();
            self._allocator.destroy(conn);

            conn_to_add = newConnection(self, true) catch |err1| {
                // we failed to create the connection, track it as missing and let
                // the background reconnector try
                self._mutex.lockUncancelable(self._io);
                self._missing += 1;
                self._mutex.unlock(self._io);

                self._reconnector.reconnect() catch |err2| {
                    log.err("Re-opening connection failed ({}) and background reconnector failed to start ({})", .{ err1, err2 });
                };
                return;
            };
        }

        var conns = self._conns;
        self._mutex.lockUncancelable(self._io);
        const available = self._available;
        conns[available] = conn_to_add;
        self._available = available + 1;
        self._mutex.unlock(self._io);
        self._event.set(self._io);
    }

    pub fn newListener(self: *Pool) !Listener {
        var listener = try Listener.open(self._allocator, self._io, self._opts.connect);
        try listener.auth(self._opts.auth);
        return listener;
    }

    pub fn stats(self: *Pool) Stats {
        self._mutex.lockUncancelable(self._io);
        defer self._mutex.unlock(self._io);

        const available = self._available;
        const missing = self._missing;
        const size = self._conns.len;

        return .{
            .size = size,
            .available = available,
            .missing = missing,
            .in_use = size - available - missing,
        };
    }

    pub fn exec(self: *Pool, sql: []const u8, values: anytype) !?i64 {
        return self.execOpts(sql, values, .{});
    }

    pub fn execOpts(self: *Pool, sql: []const u8, values: anytype, opts: Conn.QueryOpts) !?i64 {
        var conn = try self.acquire();
        defer self.release(conn);
        return conn.execOpts(sql, values, opts);
    }

    pub fn query(self: *Pool, sql: []const u8, values: anytype) !*Result {
        return self.queryOpts(sql, values, .{});
    }

    pub fn queryOpts(self: *Pool, sql: []const u8, values: anytype, opts_: Conn.QueryOpts) !*Result {
        var opts = opts_;
        opts.release_conn = true;
        var conn = try self.acquire();
        errdefer self.release(conn);
        return conn.queryOpts(sql, values, opts);
    }

    pub fn row(self: *Pool, sql: []const u8, values: anytype) !?QueryRow {
        return self.rowOpts(sql, values, .{});
    }

    pub fn rowUnsafe(self: *Pool, sql: []const u8, values: anytype) !?QueryRowUnsafe {
        return self.rowUnsafeOpts(sql, values, .{});
    }

    pub fn rowOpts(self: *Pool, sql: []const u8, values: anytype, opts_: Conn.QueryOpts) !?QueryRow {
        var opts = opts_;
        opts.release_conn = true;
        var conn = try self.acquire();
        return conn.rowOpts(sql, values, opts);
    }

    pub fn rowUnsafeOpts(self: *Pool, sql: []const u8, values: anytype, opts_: Conn.QueryOpts) !?QueryRowUnsafe {
        var opts = opts_;
        opts.release_conn = true;
        var conn = try self.acquire();
        return conn.rowUnsafeOpts(sql, values, opts);
    }
};

const Reconnector = struct {
    // number of connections that the pool is missing, i.e. how many need to be
    // reconnected
    count: usize,

    // when stop is called, this is set to true
    stopped: bool,

    pool: *Pool,
    mutex: Io.Mutex,

    // the thread, if any, that the monitor is running in
    future: ?Io.Future(Io.Cancelable!void),

    fn init(pool: *Pool) Reconnector {
        return .{
            .pool = pool,
            .count = 0,
            .mutex = .init,
            .stopped = false,
            .future = null,
        };
    }

    fn run(self: *Reconnector) Io.Cancelable!void {
        const pool = self.pool;
        var locked = false;

        try self.mutex.lock(self.pool._io);
        locked = true;
        defer {
            if (locked) self.mutex.unlock(self.pool._io);
        }
        loop: while (self.count > 0) {
            const stopped = self.stopped;
            self.mutex.unlock(self.pool._io);
            locked = false;
            if (stopped == true) {
                return;
            }

            const conn = newConnection(pool, false) catch {
                try std.Io.sleep(self.pool._io, .fromSeconds(2), .awake);
                try self.mutex.lock(self.pool._io);
                locked = true;
                continue :loop;
            };

            // Decrement missing count when successfully recreated
            try pool._mutex.lock(self.pool._io);
            std.debug.assert(pool._missing > 0);
            pool._missing -= 1;
            pool._mutex.unlock(self.pool._io);

            conn.release(); // inserts it into the pool
            try self.mutex.lock(self.pool._io);
            locked = true;
            self.count -= 1;
        }
    }

    fn stop(self: *Reconnector) void {
        self.mutex.lockUncancelable(self.pool._io);
        self.stopped = true;
        self.mutex.unlock(self.pool._io);
        if (self.future) |*future| {
            future.cancel(self.pool._io) catch return;
        }
    }

    fn reconnect(self: *Reconnector) !void {
        try self.mutex.lock(self.pool._io);
        defer self.mutex.unlock(self.pool._io);
        self.count += 1;
        if (self.future == null) {
            self.future = try self.pool._io.concurrent(Reconnector.run, .{self});
        }
    }
};

fn newConnection(pool: *Pool, log_failure: bool) !*Conn {
    const opts = &pool._opts;
    const allocator = pool._allocator;

    const conn = allocator.create(Conn) catch |err| {
        if (log_failure) log.err("connect error: {}", .{err});
        return err;
    };
    errdefer allocator.destroy(conn);

    conn.* = Conn.open(allocator, pool._io, opts.connect) catch |err| {
        if (log_failure) log.err("connect error: {}", .{err});
        return err;
    };
    errdefer conn.deinit();

    conn.auth(opts.auth) catch |err| {
        if (log_failure) {
            if (conn.err) |pg_err| {
                log.err("connect error: {s}", .{pg_err.message});
            } else {
                log.err("connect error: {}", .{err});
            }
        }
        return err;
    };
    conn._pool = pool;
    return conn;
}

const t = lib.testing;
test "Pool" {
    var pool = try Pool.init(t.allocator, t.io, .{
        .size = 2,
        .auth = t.authOpts(.{}),
        .connect_on_init_count = 1,
    });
    defer pool.deinit();

    {
        const c1 = try pool.acquire();
        defer pool.release(c1);
        _ = try c1.exec(
            \\ drop table if exists pool_test;
            \\ create table pool_test (id int not null)
        , .{});
    }

    var group: Io.Group = .init;
    try group.concurrent(t.io, testPool, .{pool});
    try group.concurrent(t.io, testPool, .{pool});
    try group.concurrent(t.io, testPool, .{pool});
    try group.await(t.io);

    {
        const c1 = try pool.acquire();
        defer c1.release();

        const affected = try c1.exec("delete from pool_test", .{});
        try t.expectEqual(1500, affected.?);
    }
}

test "Pool: Release" {
    var pool = try Pool.init(t.allocator, t.io, .{
        .size = 2,
        .auth = .{
            .database = "postgres",
            .username = "postgres",
            .password = "postgres",
        },
    });
    defer pool.deinit();

    const c1 = try pool.acquire();
    c1._state = .query;
    pool.release(c1);
}

test "Pool: Overflow and wait for free slot" {
    var pool = try Pool.init(t.allocator, t.io, .{
        .size = 2,
        .auth = t.authOpts(.{}),
        .timeout = .fromSeconds(1),
        .connect_on_init_count = 1,
    });
    defer pool.deinit();
    try t.expectEqual(1, pool._available);

    // Use up the pool
    const c1 = try pool.acquire();
    defer c1.release();
    c1._state = .query;
    try t.expectEqual(0, pool._available);

    const c2 = try pool.acquire();
    errdefer c2.release();
    c2._state = .query;
    try t.expectEqual(0, pool._available);

    // try to make another connect while the pool is overful
    // expect this to timeout ?
    try t.expectError(error.Timeout, pool.acquire());

    // now try to acquire in another thread, then quickly release c2
    // it should then allow the next acquire
    var future_acquire = try t.io.concurrent(Pool.acquire, .{pool});
    Io.sleep(t.io, .fromMilliseconds(100), .awake) catch {};
    c2.release();
    const delayed_connection = try future_acquire.await(t.io);
    delayed_connection.release();
}

test "Pool: stats" {
    var pool = try Pool.init(t.allocator, t.io, .{
        .size = 3,
        .auth = t.authOpts(.{}),
    });
    defer pool.deinit();

    // Initial state: all connections available
    {
        const s = pool.stats();
        try t.expectEqual(3, s.size);
        try t.expectEqual(3, s.available);
        try t.expectEqual(0, s.missing);
        try t.expectEqual(0, s.in_use);
    }

    // Acquire one connection
    const c1 = try pool.acquire();
    {
        const s = pool.stats();
        try t.expectEqual(3, s.size);
        try t.expectEqual(2, s.available);
        try t.expectEqual(0, s.missing);
        try t.expectEqual(1, s.in_use);
    }

    // Acquire another
    const c2 = try pool.acquire();
    {
        const s = pool.stats();
        try t.expectEqual(3, s.size);
        try t.expectEqual(1, s.available);
        try t.expectEqual(0, s.missing);
        try t.expectEqual(2, s.in_use);
    }

    // Release one
    pool.release(c1);
    {
        const s = pool.stats();
        try t.expectEqual(3, s.size);
        try t.expectEqual(2, s.available);
        try t.expectEqual(0, s.missing);
        try t.expectEqual(1, s.in_use);
    }

    // Release the other
    pool.release(c2);
    {
        const s = pool.stats();
        try t.expectEqual(3, s.size);
        try t.expectEqual(3, s.available);
        try t.expectEqual(0, s.missing);
        try t.expectEqual(0, s.in_use);
    }
}

test "Pool: exec" {
    var pool = try Pool.init(t.allocator, t.io, .{ .size = 1, .auth = t.authOpts(.{}) });
    defer pool.deinit();

    {
        const n = try pool.exec("insert into simple_table values ($1), ($2), ($3)", .{ "pool_insert_args_a", "pool_insert_args_b", "pool_insert_args_c" });
        try t.expectEqual(3, n.?);
    }

    {
        // this makes sure the connection was returned to the pool
        const n = try pool.exec("insert into simple_table values ($1)", .{"pool_insert_args_a"});
        try t.expectEqual(1, n.?);
    }
}

test "Pool: Query/Row" {
    var pool = try Pool.init(t.allocator, t.io, .{ .size = 1, .auth = t.authOpts(.{}) });
    defer pool.deinit();

    {
        _ = try pool.exec("insert into all_types (id, col_int8, col_text) values ($1, $2, $3)", .{ 100, 1, "val-1" });
        _ = try pool.exec("insert into all_types (id, col_int8, col_text) values ($1, $2, $3)", .{ 101, 2, "val-2" });
    }

    for (0..3) |_| {
        var result = try pool.query("select col_int8, col_text from all_types where id = any($1)", .{[2]i32{ 100, 101 }});
        defer result.deinit();

        const row1 = (try result.nextUnsafe()) orelse unreachable;
        try t.expectEqual(1, row1.get(i64, 0));
        try t.expectString("val-1", row1.get([]u8, 1));

        const row2 = (try result.nextUnsafe()) orelse unreachable;
        try t.expectEqual(2, row2.get(i64, 0));
        try t.expectString("val-2", row2.get([]u8, 1));

        try t.expectEqual(null, result.nextUnsafe());
    }

    for (0..3) |_| {
        var row = try pool.rowUnsafe("select col_int8, col_text from all_types where id = $1", .{101}) orelse unreachable;
        defer row.deinit() catch {};

        try t.expectEqual(2, row.get(i64, 0));
        try t.expectString("val-2", row.get([]u8, 1));
    }
}

test "Pool: Row error" {
    var pool = try Pool.init(t.allocator, t.io, .{ .size = 1, .auth = t.authOpts(.{}) });
    defer pool.deinit();

    _ = try pool.rowUnsafe("insert into all_types (id) values ($1)", .{200});

    // This would segfault:
    // https://github.com/karlseguin/pg.zig/issues/34
    try t.expectError(error.PG, pool.rowUnsafe("insert into all_types (id) values ($1)", .{200}));

    try t.expectEqual(1, pool._available);
}

fn testPool(p: *Pool) void {
    for (0..500) |i| {
        const conn = p.acquire() catch |err| {
            std.log.debug("testPool aquire {} error {}", .{ i, err });
            return;
        };
        _ = conn.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
        conn.release();
    }
}
