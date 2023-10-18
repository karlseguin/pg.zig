// Exposed within this library
const std = @import("std");

pub const is_test = @import("builtin").is_test;

const reader = @import("reader.zig");
pub const types = @import("types.zig");
pub const proto = @import("proto.zig");
pub const Conn = @import("conn.zig").Conn;
pub const SASL = @import("sasl.zig").SASL;
pub const Result = @import("result.zig").Result;

pub const Reader = reader.Reader;
pub const Message = reader.Message;

pub const Timeout = struct {
	const os = std.os;

	handle: c_int,

	// the wallclock that the query must end at
	deadline: i64,

	// byte representation of an os.timeval (for setsockopt)
	timeval: [@sizeOf(os.timeval)]u8,

	const zero = std.mem.toBytes(os.timeval{.tv_sec = 0, .tv_usec = 0});

	pub fn init(ms: i64, stream: anytype) Timeout {
		return .{
			.handle = stream.handle,
			.deadline = std.time.milliTimestamp() + ms,
			.timeval = std.mem.toBytes(os.timeval{
				.tv_sec = @intCast(@divTrunc(ms, 1000)),
				.tv_usec = @intCast(@mod(ms, 1000) * 1000),
			})
		};
	}

	pub fn expired(self: *const Timeout) bool {
		return std.time.milliTimestamp() > self.deadline;
	}

	pub fn forSendAndRecv(self: *const Timeout) !void {
		try Timeout.recv(self.handle, &self.timeval);
		return Timeout.send(self.handle, &self.timeval);
	}

	pub fn clear(self: *const Timeout) !void {
		try Timeout.recv(self.handle, &zero);
		return Timeout.send(self.handle, &zero);
	}

	pub fn forRecv(self: *const Timeout) !void {
		return Timeout.recv(self.handle, &self.timeval);
	}

	pub fn forSend(self: *const Timeout) !void {
		return Timeout.send(self.handle, &self.timeval);
	}

	pub fn recv(handle: c_int, timeval: *const [@sizeOf(os.timeval)]u8) !void {
		return os.setsockopt(handle, os.SOL.SOCKET, os.SO.RCVTIMEO, timeval);
	}
	pub fn send(handle: c_int, timeval: *const [@sizeOf(os.timeval)]u8) !void {
		return os.setsockopt(handle, os.SOL.SOCKET, os.SO.SNDTIMEO, timeval);
	}
};

// For every query, we need to store the type of each column (so we know
// how to parse the data). Optionally, we might need the name of each column.
// The connection has a default QueryState for a max # of columns, and we'll use
// that whenever we can. Otherwise, we'll create this dynamically.
pub const QueryState = struct {
	const Allocator = std.mem.Allocator;

	// The name for each returned column, we only populate this if we're told
	// to (since it requires us to dupe the data)
	names: [][]const u8,


	// This is different than the above. The above are set once per query
	// from the RowDescription response of our Describe message. This is set for
	// each DataRow message we receive. It maps a column position with the encoded
	// value.
	values: []Value,

	// The OID the server expects for each parameters
	param_oids: []i32,

	// The OID for each returned column
	result_oids: []i32,

	pub const Value = struct {
		is_null: bool,
		data: []const u8,
	};

	pub fn init(allocator: Allocator, size: usize) !QueryState{
		const names = try allocator.alloc([]u8, size);
		errdefer allocator.free(names);

		const values = try allocator.alloc(Value, size);
		errdefer allocator.free(values);

		const param_oids = try allocator.alloc(i32, size);
		errdefer allocator.free(param_oids);

		const result_oids = try allocator.alloc(i32, size);
		errdefer allocator.free(result_oids);

		return .{
			.names = names,
			.values = values,
			.param_oids = param_oids,
			.result_oids = result_oids,
		};
	}

	// Populates the querystate from the RowDescription payload
	// We already read the number_of_columns from data, so we pass it in here
	// We also already know that number_of_columns fits within our arrays
	pub fn from(self: *QueryState, number_of_columns: u16, data: []const u8) !void {
		// skip the column count, which we already know as number_of_columns
		var pos: usize = 2;

		for (0..number_of_columns) |i| {
			// skip the name, for now
			pos = std.mem.indexOfScalarPos(u8, data, pos, 0) orelse return error.InvalidDataRow;

			if (data.len < (pos + 19)) {
				return error.InvalidDataRow;
			}

			// skip the name null terminator (1)
			// skip the table object_id this table belongs to (4)
			// skip the attribute number of this table column (2)
			pos += 7;

			{
				const end = pos + 4;
				self.result_oids[i] = std.mem.readIntBig(i32, data[pos..end][0..4]);
				pos = end;
			}

			// skip date type size (2), type modifier (4) format code (2)
			pos += 8;
		}
	}

	pub fn deinit(self: QueryState, allocator: Allocator) void {
		allocator.free(self.names);
		allocator.free(self.values);
		allocator.free(self.param_oids);
		allocator.free(self.result_oids);
	}
};

pub const testing = @import("t.zig");

const t = testing;
test "Timeout: init" {
	const now = std.time.milliTimestamp();
	{
		const to = Timeout.init(4000, .{.handle = 0});
		try t.expectDelta(now + 4000, to.deadline, 50);
		try t.expectEqual(false, to.expired());

		const tv = std.mem.bytesToValue(std.os.timeval, &to.timeval);
		try t.expectEqual(4, tv.tv_sec);
		try t.expectEqual(0, tv.tv_usec);
	}

	{
		const to = Timeout.init(10500, .{.handle = 0});
		try t.expectDelta(now + 10500, to.deadline, 50);
		try t.expectEqual(false, to.expired());
		const tv = std.mem.bytesToValue(std.os.timeval, &to.timeval);
		try t.expectEqual(10, tv.tv_sec);
		try t.expectEqual(500000, tv.tv_usec);
	}

	{
		const to = Timeout.init(-1, .{.handle = 0});
		try t.expectEqual(true, to.expired());
	}
}
