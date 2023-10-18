const std = @import("std");
const conn = @import("conn.zig");

pub const is_test = @import("builtin").is_test;
pub const testing = @import("t.zig");

pub const Conn = conn.Conn;
pub const QueryOpts = conn.QueryOpts;
pub const ConnectOpts = conn.ConnectOpts;
pub const StartupOpts = conn.StartupOpts;

test {
	try testing.setup();
	std.testing.refAllDecls(@This());
}
