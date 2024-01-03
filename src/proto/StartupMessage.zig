const std = @import("std");
const proto = @import("_proto.zig");

const StartupMessage = @This();

protocol: []const u8 = &[_]u8{0, 3, 0, 0},
username: []const u8,
database: []const u8,

pub fn write(self: StartupMessage, buf: *proto.Buffer) !void {
	// 4 +   4        + 4      + 1 + N         + 1 + 8        + 1 + M         + 1 + 1 = 25 + N + M
	// len + protocol + "user" + 0 + $username + 0 "database" + 0 + $database + 0 + 0 + 0
	const payload_len = 25 + self.username.len + self.database.len;
	try buf.ensureTotalCapacity(payload_len);

	// this nonsense is to skip the buffers bound checking, since we've already
	// ensured the available capacity
	var view = buf.skip(payload_len) catch unreachable;
	view.writeIntBig(u32, @intCast(payload_len));
	view.write(self.protocol);
	view.write(&[_]u8{'u', 's', 'e', 'r', 0});
	view.write(self.username);
	view.writeByte(0);
	view.write(&[_]u8{'d', 'a', 't', 'a', 'b', 'a', 's', 'e', 0});
	view.write(self.database);
	view.writeByte(0);
	view.writeByte(0);
}

const t = proto.testing;
const Reader = proto.Reader;
test "StartupMessage: write" {
	var buf = try proto.Buffer.init(t.allocator, 128);
	defer buf.deinit();

	const s = StartupMessage{.username = "leto", .database = "ghanima"};
	try s.write(&buf);

	var reader = Reader.init(buf.string());
	try t.expectEqual(36, try reader.int32()); // payload length
	try t.expectEqual(196608, try reader.int32()); // protocol version
	try t.expectString("user", try reader.string());
	try t.expectString("leto", try reader.string());
	try t.expectString("database", try reader.string());
	try t.expectString("ghanima", try reader.string());
	try t.expectSlice(u8, &.{0}, reader.rest());
}
