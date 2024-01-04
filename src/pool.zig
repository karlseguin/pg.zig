const std = @import("std");
const lib = @import("lib.zig");

const log = lib.log;
const auth = lib.auth;
const Conn = lib.Conn;
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
	_reconnector: Reconnector,

	pub const Opts = struct {
		size: u16 = 10,
		auth: auth.Opts = .{},
		connect: Conn.Opts = .{},
		timeout: u32 = 10 * std.time.ms_per_s,
	};

	pub fn init(allocator: Allocator, opts: Opts) !*Pool {
		const pool = try allocator.create(Pool);
		errdefer allocator.destroy(pool);

		const size = opts.size;
		const conns = try allocator.alloc(*Conn, size);

		pool.* = .{
			._cond = .{},
			._mutex = .{},
			._opts = opts,
			._conns = conns,
			._available = size,
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

		const thread = try Thread.spawn(.{}, Reconnector.monitor, .{&pool._reconnector});
		thread.detach();

		return pool;
	}

	pub fn deinit(self: *Pool) void {
		self._reconnector.stop();
		const allocator = self._allocator;
		for (self._conns) |conn| {
			conn.deinit();
			allocator.destroy(conn);
		}
		allocator.free(self._conns);
		allocator.destroy(self);
	}

	pub fn acquire(self: *Pool) !*Conn {
		const conns = self._conns;
		self._mutex.lock();
		while (true) {
			const available = self._available;
			if (available == 0) {
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
			// conn should always be idle when being released. It's possible we can
			// recover from this (e.g. maybe we just need to read until we get a
			// ReadyForQuery), but we wouldn't want to block for too long. For now,
			// we'll just replace the connection.
			conn.deinit();
			self._allocator.destroy(conn);

			conn_to_add = newConnection(self, true) catch {
				// we failed to create the connection then and there, signal the
				// reconnect that it has a connection to reconnect
				self._reconnector.reconnect();
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
};

const Reconnector = struct {
	// number of connections that the pool is missing, i.e. how many need to be
	// reconnected
	count: usize,

	// when stop is called, this is set to false, and the reconnect loop will exit
	running: bool,

	pool: *Pool,
	mutex: Thread.Mutex,
	cond: Thread.Condition,

	fn init(pool: *Pool) Reconnector {
		return .{
			.pool = pool,
			.count = 0,
			.cond = .{},
			.mutex = .{},
			.running = false,
		};
	}

	fn monitor(self: *Reconnector) void {
		self.running = true;
		const pool = self.pool;
		const retry_delay = 5 * std.time.ns_per_s;

		self.mutex.lock();
		while (true) {
			if (self.running == false) {
				self.mutex.unlock();
				return;
			}

			self.cond.wait(&self.mutex);
			loop: while (self.count > 0) {
				const running = self.running;
				self.mutex.unlock();
				if (running == false) {
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
	}

	fn stop(self: *Reconnector) void {
		self.mutex.lock();
		self.running = false;
		self.mutex.unlock();
		self.cond.signal();
	}

	fn reconnect(self: *Reconnector) void {
		self.mutex.lock();
		self.count += 1;
		self.mutex.unlock();
		self.cond.signal();
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

	t1.join(); t2.join(); t3.join();

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

fn testPool(p: *Pool) void {
	for (0..500) |i| {
		const conn = p.acquire() catch unreachable;
		_ = conn.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
		conn.release();
	}
}
