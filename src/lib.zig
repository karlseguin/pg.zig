// Exposed within this library
const std = @import("std");

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
