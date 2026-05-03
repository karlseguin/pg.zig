const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
const StartupMessage = @This();

protocol: []const u8 = &.{ 0, 3, 0, 0 },
username: []const u8,
database: []const u8,
application_name: ?[]const u8 = null,
params: ?std.StringHashMap([]const u8) = null,

pub fn write(self: StartupMessage, view: *Io.Writer) !void {
    // 4 +   4        + 4      + 1 + N         + 1 + 8        + 1 + M         + 1 + 1 = 25 + N + M
    // len + protocol + "user" + 0 + $username + 0 "database" + 0 + $database + 0 + 0 + 0
    var payload_len = 25 + self.username.len + self.database.len;
    if (self.params) |p| {
        var it = p.iterator();
        while (it.next()) |kv| {
            // +2 because both key and value are null-terminated
            payload_len += kv.key_ptr.len + kv.value_ptr.len + 2;
        }
    }
    if (self.application_name) |an| {
        // +2 because both key and value are null-terminated
        payload_len += "application_name".len + an.len + 2;
    }

    try view.writeInt(u32, @intCast(payload_len), .big);
    try view.writeAll(self.protocol);
    try view.writeAll(&.{ 'u', 's', 'e', 'r', 0 });
    try view.writeAll(self.username);
    try view.writeByte(0);
    try view.writeAll(&.{ 'd', 'a', 't', 'a', 'b', 'a', 's', 'e', 0 });
    try view.writeAll(self.database);
    try view.writeByte(0);
    if (self.application_name) |an| {
        try view.writeAll("application_name");
        try view.writeByte(0);
        try view.writeAll(an);
        try view.writeByte(0);
    }
    if (self.params) |p| {
        var it = p.iterator();
        while (it.next()) |kv| {
            try view.writeAll(kv.key_ptr.*);
            try view.writeByte(0);
            try view.writeAll(kv.value_ptr.*);
            try view.writeByte(0);
        }
    }
    try view.writeByte(0);
    try view.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "StartupMessage: write" {
    var buf: Io.Writer.Allocating = .init(t.allocator);
    defer buf.deinit();

    const s = StartupMessage{ .username = "leto", .database = "ghanima" };
    try s.write(&buf.writer);

    var reader = Reader.init(buf.written());
    try t.expectEqual(36, try reader.int32()); // payload length
    try t.expectEqual(196608, try reader.int32()); // protocol version
    try t.expectString("user", try reader.string());
    try t.expectString("leto", try reader.string());
    try t.expectString("database", try reader.string());
    try t.expectString("ghanima", try reader.string());
    try t.expectSlice(u8, &.{0}, reader.rest());
}
