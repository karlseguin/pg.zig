const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
const Describe = @This();

name: []const u8 = "",
type: Type = .portal,

pub const Type = enum {
    portal,
    prepared_statement,
};

pub fn write(self: Describe, w: *Io.Writer) !void {
    // 4   + 1     + N    + 1
    // len + $type + $name + 0
    const payload_len = 6 + self.name.len;

    try w.writeByte('D');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeByte(switch (self.type) {
        .portal => 'P',
        .prepared_statement => 'S',
    });

    try w.writeAll(self.name);
    try w.writeByte(0);
    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "Describe: write portal no name" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    const p = Describe{};
    try p.write(&w);

    var reader = Reader.init(w.buffered());
    try t.expectEqual('D', try reader.byte());
    try t.expectEqual(6, try reader.int32()); // payload length
    try t.expectEqual('P', try reader.byte());
    try t.expectString("", try reader.restAsString());
}

test "Describe: write prepared statement with name" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    const p = Describe{ .type = .prepared_statement, .name = "the-name" };
    try p.write(&w);

    var reader = Reader.init(w.buffered());
    try t.expectEqual('D', try reader.byte());
    try t.expectEqual(14, try reader.int32()); // payload length
    try t.expectEqual('S', try reader.byte());
    try t.expectString("the-name", try reader.restAsString());
}
