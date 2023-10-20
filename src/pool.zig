const std = @import("std");
const lib = @import("lib.zig");

const Conn = lib.Conn;

const Allocator = std.mem.Allocator;

pub const log = std.log.scoped(.pg);

pub const Pool = struct {
	_timeout: u64,
	_conns: []*Conn,
	_available: usize,
	_allocator: Allocator,
	_mutex: std.Thread.Mutex,
	_cond: std.Thread.Condition,

	pub const PoolOpts = struct {
		size: u16 = 10,
		timeout: u32 = 10 * std.time.ms_per_s,
		connect: Conn.ConnectOpts = .{},
		startup: Conn.StartupOpts = .{},
	};

	pub fn init(allocator: Allocator, opts: PoolOpts) !Pool {
		const size = opts.size;
		const conns = try allocator.alloc(*Conn, size);

		var opened_connections: usize = 0;
		errdefer {
			for (0..opened_connections) |i| {
				conns[i].deinit();
			}
		}

		for (0..size) |i| {
			const conn = try allocator.create(Conn);

			conn.* = Conn.open(allocator, opts.connect) catch |err| {
				allocator.destroy(conn);
				return err;
			};
			opened_connections += 1;
			conns[i] = conn;

			conn.startup(opts.startup) catch |err| {
				if (conn.err) |pg_err| {
					log.err("failed to authenticate: {s}", .{pg_err.message});
				}
				return err;
			};
		}

		return .{
			._conns = conns,
			._available = size,
			._allocator = allocator,
			._mutex = std.Thread.Mutex{},
			._cond = std.Thread.Condition{},
			._timeout = @as(u64, @intCast(opts.timeout)) * std.time.ns_per_ms,
		};
	}


	pub fn deinit(self: *Pool) void {
		const allocator = self._allocator;
		for (self._conns) |conn| {
			conn.deinit();
			allocator.destroy(conn);
		}
		allocator.free(self._conns);
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
		var conns = self._conns;

		self._mutex.lock();
		const available = self._available;
		conns[available] = conn;
		self._available = available + 1;
		self._mutex.unlock();
		self._cond.signal();
	}
};

const t = lib.testing;
// test "Pool" {
// 	var pool = try Pool.init(t.allocator, .{
// 		.size = 2,
// 		.startup = .{
// 			.database = "postgres",
// 			.username = "postgres",
// 			.password = "root_pw",
// 		},
// 	});
// 	defer pool.deinit();

// 	{
// 		const c1 = try pool.acquire();
// 		defer pool.release(c1);
// 		_ = try c1.exec(
// 			\\ drop table if exists pool_test;
// 			\\ create table pool_test (id int not null)
// 		, .{});
// 	}

// 	const t1 = try std.Thread.spawn(.{}, testPool, .{&pool});
// 	const t2 = try std.Thread.spawn(.{}, testPool, .{&pool});
// 	const t3 = try std.Thread.spawn(.{}, testPool, .{&pool});

// 	t1.join(); t2.join(); t3.join();

// 	{
// 		const c1 = try pool.acquire();
// 		defer pool.release(c1);

// 		const affected = try c1.exec("delete from pool_test", .{});
// 		try t.expectEqual(6000, affected.?);
// 	}
// }

// fn testPool(p: *Pool) void {
// 	for (0..2000) |i| {
// 		const conn = p.acquire() catch unreachable;
// 		_ = conn.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
// 		p.release(conn);
// 	}
// }
