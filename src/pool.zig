const std = @import("std");
const lib = @import("lib.zig");

const log = lib.log;
const Conn = lib.Conn;
const Result = lib.Result;
const SSLCtx = lib.SSLCtx;
const QueryRow = lib.QueryRow;
const QueryRowUnsafe = lib.QueryRowUnsafe;
const Listener = @import("listener.zig").Listener;

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Io = std.Io;

pub const Pool = struct {
    _io: Io,
    _opts: Opts,
    _timeout: u64,
    _conns: []*Conn,
    _available: usize,
    _missing: usize,
    _allocator: Allocator,
    _mutex: Io.Mutex,
    _cond: Io.Condition,
    _ssl_ctx: ?*lib.SSLCtx,
    _reconnector: Reconnector,
    // not to be used outside of init
    _arena: ArenaAllocator,

    pub const Opts = struct {
        size: u16 = 10,
        auth: Conn.AuthOpts = .{},
        connect: Conn.Opts = .{},
        timeout: u32 = 10 * std.time.ms_per_s,
        connect_on_init_count: ?u16 = null,
    };

    pub const Stats = struct {
        size: usize,
        available: usize,
        missing: usize,
        in_use: usize,
    };

    pub fn initUri(io: Io, allocator: Allocator, uri: std.Uri, opts: Opts) !*Pool {
        var po = try lib.parseOpts(uri, allocator);
        // po.opts references memory owned by po.arena (or by `uri`). init dupes
        // everything it needs into the pool's own arena, so this temporary arena
        // can be freed as soon as init returns.
        defer po.deinit();
        po.opts.size = opts.size;
        po.opts.timeout = opts.timeout;
        return init(io, allocator, po.opts);
    }

    pub fn init(io: Io, allocator: Allocator, opts: Opts) !*Pool {
        var arena = ArenaAllocator.init(allocator);
        errdefer arena.deinit();

        const aa = arena.allocator();
        const pool = try aa.create(Pool);
        const size = opts.size;
        const conns = try aa.alloc(*Conn, size);

        // Copy every caller-provided string into our arena so the pool owns them
        // outright. Callers (including initUri) don't need to keep `opts`'s strings
        // alive past this call.
        var opts_copy = opts;
        opts_copy.auth.username = try aa.dupe(u8, opts.auth.username);
        if (opts.auth.password) |v| opts_copy.auth.password = try aa.dupe(u8, v);
        if (opts.auth.database) |v| opts_copy.auth.database = try aa.dupe(u8, v);
        if (opts.auth.application_name) |v| opts_copy.auth.application_name = try aa.dupe(u8, v);
        if (opts.connect.host) |v| opts_copy.connect.host = try aa.dupe(u8, v);
        // Note: auth.startup_parameters (a StringHashMap) is not deep-copied; it is
        // currently unused, but if it ever gets wired up it must be owned here too.

        var ssl_ctx: ?*SSLCtx = null;
        if (comptime lib.has_openssl) {
            switch (opts.connect.tls) {
                .off => {},
                else => |tls_config| {
                    if (opts_copy.connect.host) |h| {
                        opts_copy.connect._hostz = try aa.dupeZ(u8, h);
                    }
                    // the cert path is re-read on every (re)connect, so own it too
                    switch (tls_config) {
                        .verify_full => |path| if (path) |p| {
                            opts_copy.connect.tls = .{ .verify_full = try aa.dupe(u8, p) };
                        },
                        else => {},
                    }
                    ssl_ctx = try lib.initializeSSLContext(tls_config);
                },
            }
        }
        errdefer lib.freeSSLContext(ssl_ctx);
        const connect_on_init_count = opts.connect_on_init_count orelse size;

        pool.* = .{
            ._io = io,
            ._cond = .init,
            ._mutex = .init,
            ._conns = conns,
            ._arena = arena,
            ._opts = opts_copy,
            ._ssl_ctx = ssl_ctx,
            ._missing = 0,
            ._allocator = allocator,
            ._available = connect_on_init_count,
            ._reconnector = Reconnector.init(pool),
            ._timeout = @as(u64, @intCast(opts.timeout)) * std.time.ns_per_ms,
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

        errdefer pool._reconnector.stop();

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
        for (self._conns[0..self._available]) |conn| {
            conn.deinit();
            allocator.destroy(conn);
        }
        lib.freeSSLContext(self._ssl_ctx);
        self._arena.deinit();
    }

    pub fn acquire(self: *Pool) !*Conn {
        const conns = self._conns;
        const io = self._io;
        const deadline = @as(i64, @intCast(self._timeout));
        const start = std.Io.Timestamp.now(io, .awake);

        try self._mutex.lock(io);
        errdefer self._mutex.unlock(io);

        const SelectResult = union(enum) { t: Io.Cancelable!void, c: Io.Cancelable!void };
        var select_buf: [1]SelectResult = undefined;

        while (true) {
            const available = self._available;
            const missing = self._missing;

            if (available == 0) {
                // Check if pool is completely exhausted
                const total_alive = self._conns.len - missing;
                if (total_alive == 0) {
                    return error.PoolExhausted;
                }

                lib.metrics.poolEmpty();

                // Calculate remaining timeout
                const now = std.Io.Timestamp.now(io, .awake);
                const elapsed = start.durationTo(now).toNanoseconds();
                if (elapsed >= deadline) {
                    return error.Timeout;
                }

                const remaining_ns = deadline - elapsed;

                var select: Io.Select(SelectResult) = .init(io, &select_buf);
                defer select.cancelDiscard();
                try select.concurrent(.t, Io.sleep, .{ io, .fromNanoseconds(remaining_ns), .awake });
                try select.concurrent(.c, Io.Condition.wait, .{ &self._cond, io, &self._mutex });

                _ = try select.await();
                continue;
            }

            const index = available - 1;
            const conn = conns[index];
            self._available = index;
            self._mutex.unlock(io);
            return conn;
        }
    }

    pub fn release(self: *Pool, conn: *Conn) void {
        var conn_to_add = conn;
        const io = self._io;

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
                self._mutex.lockUncancelable(io);
                self._missing += 1;
                self._mutex.unlock(io);

                self._reconnector.reconnect() catch |err2| {
                    log.err("Re-opening connection failed ({}) and background reconnector failed to start ({})", .{ err1, err2 });
                };
                return;
            };
        }

        var conns = self._conns;
        self._mutex.lockUncancelable(io);
        const available = self._available;
        conns[available] = conn_to_add;
        self._available = available + 1;
        self._mutex.unlock(io);
        self._cond.signal(io);
    }

    pub fn newListener(self: *Pool) !Listener {
        var listener = try Listener.open(self._io, self._allocator, self._opts.connect);
        try listener.auth(self._opts.auth);
        return listener;
    }

    pub fn stats(self: *Pool) Stats {
        const io = self._io;
        self._mutex.lockUncancelable(io);
        defer self._mutex.unlock(io);

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

    // true while the reconnector task is running
    running: bool,

    pool: *Pool,
    mutex: Io.Mutex,

    // owns the reconnector task, if any
    group: Io.Group,

    fn init(pool: *Pool) Reconnector {
        return .{
            .pool = pool,
            .count = 0,
            .mutex = .init,
            .group = .init,
            .stopped = false,
            .running = false,
        };
    }

    fn run(self: *Reconnector) Io.Cancelable!void {
        const pool = self.pool;
        const io = pool._io;
        const retry_delay: std.Io.Duration = .fromSeconds(2);

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        defer self.running = false;

        // the mutex is held whenever the loop condition is evaluated
        while (self.stopped == false and self.count > 0) {
            {
                self.mutex.unlock(io);
                defer self.mutex.lockUncancelable(io);

                const conn = newConnection(pool, false) catch |err| {
                    if (err == error.Canceled) return error.Canceled;
                    try std.Io.sleep(io, retry_delay, .awake);
                    continue;
                };

                // Decrement missing count when successfully recreated
                pool._mutex.lockUncancelable(io);
                std.debug.assert(pool._missing > 0);
                pool._missing -= 1;
                pool._mutex.unlock(io);

                conn.release(); // inserts it into the pool
            }
            self.count -= 1;
        }
    }

    fn stop(self: *Reconnector) void {
        const io = self.pool._io;

        self.mutex.lockUncancelable(io);
        self.stopped = true;
        self.mutex.unlock(io);

        self.group.cancel(io);
    }

    fn reconnect(self: *Reconnector) !void {
        const io = self.pool._io;

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        if (self.stopped == true) {
            return;
        }

        self.count += 1;
        if (self.running == true) {
            // the running task will pick up the new count
            return;
        }

        // the task blocks on our mutex, so it can't clear `running` before we set it
        try self.group.concurrent(io, Reconnector.run, .{self});
        self.running = true;
    }
};

fn newConnection(pool: *Pool, log_failure: bool) !*Conn {
    const opts = &pool._opts;
    const allocator = pool._allocator;
    const io = pool._io;

    const conn = allocator.create(Conn) catch |err| {
        if (log_failure) log.err("connect error: {}", .{err});
        return err;
    };
    errdefer allocator.destroy(conn);

    conn.* = Conn.open(io, allocator, opts.connect) catch |err| {
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
    var pool = try Pool.init(t.io, t.allocator, .{
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

    const t1 = try std.Thread.spawn(.{}, testPool, .{pool});
    const t2 = try std.Thread.spawn(.{}, testPool, .{pool});
    const t3 = try std.Thread.spawn(.{}, testPool, .{pool});

    t1.join();
    t2.join();
    t3.join();

    {
        const c1 = try pool.acquire();
        defer c1.release();

        const affected = try c1.exec("delete from pool_test", .{});
        try t.expectEqual(1500, affected.?);
    }
}

test "Pool: deinit while the reconnector is retrying" {
    const io = t.io;

    // bound but never listening, so connects are refused. Holding it for the
    // duration of the test keeps anything else off the port.
    const addr: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    const socket = try addr.bind(io, .{ .mode = .stream });
    defer socket.close(io);

    var pool = try Pool.init(io, t.allocator, .{
        .size = 1,
        .connect_on_init_count = 0,
        .connect = .{ .port = socket.address.ip4.port, .host = "127.0.0.1" },
        .auth = t.authOpts(.{}),
    });
    try std.Io.sleep(io, .fromNanoseconds(100 * std.time.ns_per_ms), .awake);

    // deinit cancels the backoff rather than waiting the full 2s
    const start = std.Io.Timestamp.now(io, .awake);
    pool.deinit();
    const elapsed = start.durationTo(std.Io.Timestamp.now(io, .awake)).toNanoseconds();
    try t.expectEqual(true, elapsed < std.time.ns_per_s);
}

test "Pool: Release" {
    var pool = try Pool.init(t.io, t.allocator, .{
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

test "Pool: stats" {
    var pool = try Pool.init(t.io, t.allocator, .{
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
    var pool = try Pool.init(t.io, t.allocator, .{ .size = 1, .auth = t.authOpts(.{}) });
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
    var pool = try Pool.init(t.io, t.allocator, .{ .size = 1, .auth = t.authOpts(.{}) });
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
    var pool = try Pool.init(t.io, t.allocator, .{ .size = 1, .auth = t.authOpts(.{}) });
    defer pool.deinit();

    _ = try pool.rowUnsafe("insert into all_types (id) values ($1)", .{200});

    // This would segfault:
    // https://github.com/karlseguin/pg.zig/issues/34
    try t.expectError(error.PG, pool.rowUnsafe("insert into all_types (id) values ($1)", .{200}));

    try t.expectEqual(1, pool._available);
}

test "Pool: init owns its connection strings" {
    // Heap-allocate the auth strings and free them right after init to prove the
    // pool kept its own copies and doesn't depend on the caller's `opts`.
    const username = try t.allocator.dupe(u8, "postgres");
    const password = try t.allocator.dupe(u8, "postgres");
    const database = try t.allocator.dupe(u8, "postgres");
    const host = try t.allocator.dupe(u8, "127.0.0.1");

    var pool = try Pool.init(t.io, t.allocator, .{
        .size = 2,
        .auth = .{ .username = username, .password = password, .database = database },
        .connect = .{ .host = host },
    });
    defer pool.deinit();

    t.allocator.free(username);
    t.allocator.free(password);
    t.allocator.free(database);
    t.allocator.free(host);

    try forceReconnect(pool);
}

test "Pool: initUri owns its connection strings" {
    // Heap-allocate the URI string and free it right after init to prove the pool
    // doesn't retain pointers into it. %73 == 's': decodes to "postgres" while also
    // forcing Uri to allocate a decoded copy into the parse arena.
    const uri_str = try t.allocator.dupe(u8, "postgresql://postgre%73:postgres@127.0.0.1:5432/postgres");
    const uri = try std.Uri.parse(uri_str);

    var pool = try Pool.initUri(t.io, t.allocator, uri, .{ .size = 2 });
    defer pool.deinit();

    t.allocator.free(uri_str);

    try forceReconnect(pool);
}

fn testPool(p: *Pool) void {
    for (0..500) |i| {
        const conn = p.acquire() catch unreachable;
        _ = conn.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
        conn.release();
    }
}

// forces release() to discard the connection and open a fresh one, exercising
// reconnect with the pool's stored auth strings.
fn forceReconnect(pool: *Pool) !void {
    const c1 = try pool.acquire();
    c1._state = .query;
    pool.release(c1);

    const c2 = try pool.acquire();
    defer pool.release(c2);
    _ = try c2.exec("select 1", .{});
}
