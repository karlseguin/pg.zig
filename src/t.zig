const std = @import("std");

const Allocator = std.mem.Allocator;
const Conn = @import("conn.zig").Conn;

pub const allocator = std.testing.allocator;

// std.testing.expectEqual won't coerce expected to actual, which is a problem
// when expected is frequently a comptime.
// https://github.com/ziglang/zig/issues/4437
pub fn expectEqual(expected: anytype, actual: anytype) !void {
	try std.testing.expectEqual(@as(@TypeOf(actual), expected), actual);
}
pub fn expectDelta(expected: anytype, actual: @TypeOf(expected), delta: @TypeOf(expected)) !void {
	try expectEqual(true, expected - delta <= actual);
	try expectEqual(true, expected + delta >= actual);
}
pub const expectError = std.testing.expectError;
pub const expectSlice = std.testing.expectEqualSlices;
pub const expectString = std.testing.expectEqualStrings;

pub fn getRandom() std.rand.DefaultPrng {
	var seed: u64 = undefined;
	std.os.getrandom(std.mem.asBytes(&seed)) catch unreachable;
	return std.rand.DefaultPrng.init(seed);
}

pub fn setup() !void {
	var c = connect(.{});
	defer c.deinit();
	_ = c.exec(
		\\ drop user if exists pgz_user_nopass;
		\\ drop user if exists pgz_user_clear;
		\\ drop user if exists pgz_user_scram_sha256;
		\\ create user pgz_user_nopass;
		\\ create user pgz_user_clear with password 'pgz_user_clear_pw';
		\\ create user pgz_user_scram_sha256 with password 'pgz_user_scram_sha256_pw';
	, .{}) catch |err| try fail(c, err);

	_ = c.exec(
		\\ drop table if exists simple_table;
		\\ create table simple_table (value text);
	, .{}) catch |err| try fail(c, err);

	_ = c.exec(
		\\ drop type if exists custom_enum cascade;
		\\ create type custom_enum as enum ('val1', 'val2');
	, .{}) catch |err| try fail(c, err);

	_ = c.exec(
		\\ drop table if exists all_types;
		\\ create table all_types (
		\\   id integer primary key,
		\\   col_int2 smallint,
		\\   col_int4 integer,
		\\   col_int8 bigint,
		\\   col_float4 float4,
		\\   col_float8 float8,
		\\   col_bool bool,
		\\   col_text text,
		\\   col_bytea bytea,
		\\   col_int2_arr smallint[],
		\\   col_int4_arr integer[],
		\\   col_int8_arr bigint[],
		\\   col_float4_arr float4[],
		\\   col_float8_arr float[],
		\\   col_bool_arr bool[],
		\\   col_text_arr text[],
		\\   col_bytea_arr bytea[],
		\\   col_enum custom_enum,
		\\   col_enum_arr custom_enum[],
		\\   col_uuid uuid,
		\\   col_uuid_arr uuid[]
		\\ );
	, .{}) catch |err| try fail(c, err);
}

// Dummy net.Stream, lets us setup data to be read and capture data that is written.
pub const Stream = struct {
	closed: bool,
	_read_index: usize,
	handle: c_int = 0,
	_to_read: std.ArrayList(u8),
	_received: std.ArrayList(u8),

	pub fn init() *Stream {
		const s = allocator.create(Stream) catch unreachable;
		s.* = .{
			.closed = false,
			._read_index = 0,
			._to_read = std.ArrayList(u8).init(allocator),
			._received = std.ArrayList(u8).init(allocator),
		};
		return s;
	}

	pub fn deinit(self: *Stream) void {
		self._to_read.deinit();
		self._received.deinit();
		allocator.destroy(self);
	}

	pub fn reset(self: *Stream) void {
		self._read_index = 0;
		self._to_read.clearRetainingCapacity();
		self._received.clearRetainingCapacity();
	}

	pub fn received(self: *Stream) []const u8 {
		return self._received.items;
	}

	pub fn add(self: *Stream, value: []const u8) void {
		self._to_read.appendSlice(value) catch unreachable;
	}

	pub fn read(self: *Stream, buf: []u8) !usize {
		std.debug.assert(!self.closed);

		const read_index = self._read_index;
		const items = self._to_read.items;

		if (read_index == items.len) {
			return 0;
		}
		if (buf.len == 0) {
			return 0;
		}

		// let's fragment this message
		const left_to_read = items.len - read_index;
		const max_can_read = if (buf.len < left_to_read) buf.len else left_to_read;

		const to_read = max_can_read;
		var data = items[read_index..(read_index+to_read)];
		if (data.len > buf.len) {
			// we have more data than we have space in buf (our target)
			// we'll give it when it can take
			data = data[0..buf.len];
		}
		self._read_index = read_index + data.len;

		for (data, 0..) |b, i| {
			buf[i] = b;
		}

		return data.len;
	}

	// store messages that are written to the stream
	pub fn writeAll(self: *Stream, data: []const u8) !void {
		self._received.appendSlice(data) catch unreachable;
	}

	pub fn close(self: *Stream) void {
		self.closed = true;
	}
};

pub fn connect(opts: anytype) Conn {
	const T = @TypeOf(opts);

	var c = Conn.open(allocator, .{}) catch unreachable;
	c.startup(.{
		.database = if (@hasField(T, "database")) opts.database else "postgres",
		.username = if (@hasField(T, "username")) opts.username else "postgres",
		.password = if (@hasField(T, "password")) opts.password else "root_pw",
	}) catch |err| {
		if (c.err) |pg| {
			@panic(pg.message);
		}
		@panic(@errorName(err));
	};
	return c;
}

pub fn fail(c: Conn, err: anyerror) !void {
	if (c.err) |pg_err| {
		std.debug.print("PG ERROR: {s}\n", .{pg_err.message});
	}
	return err;
}
