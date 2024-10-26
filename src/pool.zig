const std = @import("std");
const lib = @import("lib.zig");

const log = lib.log;
const auth = lib.auth;
const Conn = lib.Conn;
const Result = lib.Result;
const SSLCtx = lib.SSLCtx;
const QueryRow = lib.QueryRow;
const Listener = @import("listener.zig").Listener;

const Thread = std.Thread;
const Allocator = std.mem.Allocator;

pub const Pool = struct {
    _opts: Opts,
    _timeout: u64,
    _conns: []*Conn,
    _available: usize,
    _allocator: Allocator,
    _mutex: Thread.Mutex,
    _cond: Thread.Condition,
    _ssl_ctx: ?*lib.SSLCtx,
    _reconnector: Reconnector,
    _arena: std.heap.ArenaAllocator,

    pub const Opts = struct {
        size: u16 = 10,
        auth: auth.Opts = .{},
        connect: Conn.Opts = .{},
        timeout: u32 = 10 * std.time.ms_per_s,
    };

    pub fn initUri(allocator: Allocator, uri: std.Uri, size: u16, pool_timeout_ms: u32) !*Pool {
        var po = try lib.parseOpts(uri, allocator, size, pool_timeout_ms);
        defer po.deinit();
        return Pool.init(allocator, po.opts);
    }

    pub fn init(allocator: Allocator, opts: Opts) !*Pool {
        var arena = std.heap.ArenaAllocator.init(allocator);
        const aa = arena.allocator();
        errdefer arena.deinit();

        const pool = try aa.create(Pool);
        const size = opts.size;
        const conns = try aa.alloc(*Conn, size);

        var opts_copy = opts;
        var ssl_ctx: ?*SSLCtx = null;
        if (comptime lib.has_openssl) {
            if (opts.connect.tls) {
                ssl_ctx = try lib.initializeSSLContext();
            }
            if (opts.connect.host) |h| {
                opts_copy.connect._hostz = try aa.dupeZ(u8, h);
            }
        }
        errdefer lib.freeSSLContext(ssl_ctx);

        pool.* = .{
            ._cond = .{},
            ._mutex = .{},
            ._conns = conns,
            ._arena = arena,
            ._opts = opts_copy,
            ._available = size,
            ._ssl_ctx = ssl_ctx,
            ._allocator = allocator,
            ._reconnector = Reconnector.init(pool),
            ._timeout = @as(u64, @intCast(opts.timeout)) * std.time.ns_per_ms,
        };

        var opened_connections: usize = 0;
        errdefer {
            for (0..opened_connections) |i| {
                conns[i].deinit();
            }
        }

        for (0..size) |i| {
            conns[i] = try newConnection(pool, true);
            opened_connections += 1;
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
        const conns = self._conns;
        self._mutex.lock();
        while (true) {
            const available = self._available;
            if (available == 0) {
                lib.metrics.poolEmpty();
                try self._cond.timedWait(&self._mutex, self._timeout);
                continue;
            }
            const index = available - 1;
            const conn = conns[index];
            self._available = index;
            self._mutex.unlock();
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
                // we failed to create the connection, let the background reconnector try
                self._reconnector.reconnect() catch |err2| {
                    log.err("Re-opening connection failed ({}) and background reconnector failed to start ({}) ", .{ err1, err2 });
                };
                return;
            };
        }

        var conns = self._conns;
        self._mutex.lock();
        const available = self._available;
        conns[available] = conn_to_add;
        self._available = available + 1;
        self._mutex.unlock();
        self._cond.signal();
    }

    pub fn newListener(self: *Pool) !Listener {
        var listener = try Listener.open(self._allocator, self._opts.connect);
        try listener.auth(self._opts.auth);
        return listener;
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

    pub fn rowOpts(self: *Pool, sql: []const u8, values: anytype, opts_: Conn.QueryOpts) !?QueryRow {
        var opts = opts_;
        opts.release_conn = true;
        var conn = try self.acquire();
        return conn.rowOpts(sql, values, opts);
    }
};

const Reconnector = struct {
    // number of connections that the pool is missing, i.e. how many need to be
    // reconnected
    count: usize,

    // when stop is called, this is set to true
    stopped: bool,

    pool: *Pool,
    mutex: Thread.Mutex,

    // the thread, if any, that the monitor is running in
    thread: ?Thread,

    fn init(pool: *Pool) Reconnector {
        return .{
            .pool = pool,
            .count = 0,
            .mutex = .{},
            .stopped = false,
            .thread = null,
        };
    }

    fn run(self: *Reconnector) void {
        const pool = self.pool;
        const retry_delay = 2 * std.time.ns_per_s;

        self.mutex.lock();
        loop: while (self.count > 0) {
            const stopped = self.stopped;
            self.mutex.unlock();
            if (stopped == true) {
                return;
            }

            const conn = newConnection(pool, false) catch {
                std.time.sleep(retry_delay);
                self.mutex.lock();
                continue :loop;
            };

            conn.release(); // inserts it into the pool
            self.mutex.lock();
            self.count -= 1;
        }
    }

    fn stop(self: *Reconnector) void {
        self.mutex.lock();
        self.stopped = true;
        self.mutex.unlock();
        if (self.thread) |thrd| {
            thrd.join();
        }
    }

    fn reconnect(self: *Reconnector) !void {
        self.mutex.lock();
        self.count += 1;
        if (self.thread == null) {
            self.thread = try Thread.spawn(.{ .stack_size = 1024 * 1024 }, Reconnector.run, .{self});
        }
        self.mutex.unlock();
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

    conn.* = Conn.open(allocator, opts.connect) catch |err| {
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
    var pool = try Pool.init(t.allocator, .{
        .size = 2,
        .auth = t.authOpts(.{}),
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

test "Pool: Release" {
    var pool = try Pool.init(t.allocator, .{
        .size = 2,
        .auth = .{
            .database = "postgres",
            .username = "postgres",
            .password = "root_pw",
        },
    });
    defer pool.deinit();

    const c1 = try pool.acquire();
    c1._state = .query;
    pool.release(c1);
}

test "Pool: exec" {
    var pool = try Pool.init(t.allocator, .{ .size = 1, .auth = t.authOpts(.{}) });
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
    var pool = try Pool.init(t.allocator, .{ .size = 1, .auth = t.authOpts(.{}) });
    defer pool.deinit();

    {
        _ = try pool.exec("insert into all_types (id, col_int8, col_text) values ($1, $2, $3)", .{ 100, 1, "val-1" });
        _ = try pool.exec("insert into all_types (id, col_int8, col_text) values ($1, $2, $3)", .{ 101, 2, "val-2" });
    }

    for (0..3) |_| {
        var result = try pool.query("select col_int8, col_text from all_types where id = any($1)", .{[2]i32{ 100, 101 }});
        defer result.deinit();

        const row1 = (try result.next()) orelse unreachable;
        try t.expectEqual(1, row1.get(i64, 0));
        try t.expectString("val-1", row1.get([]u8, 1));

        const row2 = (try result.next()) orelse unreachable;
        try t.expectEqual(2, row2.get(i64, 0));
        try t.expectString("val-2", row2.get([]u8, 1));

        try t.expectEqual(null, result.next());
    }

    for (0..3) |_| {
        var row = try pool.row("select col_int8, col_text from all_types where id = $1", .{101}) orelse unreachable;
        defer row.deinit() catch {};

        try t.expectEqual(2, row.get(i64, 0));
        try t.expectString("val-2", row.get([]u8, 1));
    }
}

test "Pool: Row error" {
    var pool = try Pool.init(t.allocator, .{ .size = 1, .auth = t.authOpts(.{}) });
    defer pool.deinit();

    _ = try pool.row("insert into all_types (id) values ($1)", .{200});

    // This would segfault:
    // https://github.com/karlseguin/pg.zig/issues/34
    try t.expectError(error.PG, pool.row("insert into all_types (id) values ($1)", .{200}));

    try t.expectEqual(1, pool._available);
}

fn testPool(p: *Pool) void {
    for (0..500) |i| {
        const conn = p.acquire() catch unreachable;
        _ = conn.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
        conn.release();
    }
}
