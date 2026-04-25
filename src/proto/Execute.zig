const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
const Execute = @This();

portal: []const u8 = "",
// 0 == no limit, don't use a nullable, since that would imply
// that 0 means something else.
max_rows: u32 = 0,

pub fn write(self: Execute, w: *Io.Writer) !void {
    // 4   + N       + 1 + 4
    // len + $portal + 0 + $max_rows
    const payload_len = 9 + self.portal.len;

    // // +1 for the type field, 'P'
    // const total_length = payload_len + 1;

    // try buf.ensureTotalCapacity(total_length);

    // var w = buf.skip(total_length) catch unreachable;
    try w.writeByte('E');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(self.portal);
    try w.writeByte(0);
    try w.writeInt(u32, self.max_rows, .big);
    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "Execute: write no name" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    const e = Execute{};
    try e.write(&w);

    var reader = Reader.init(w.buffered());
    try t.expectEqual('E', try reader.byte());
    try t.expectEqual(9, try reader.int32()); // payload length
    try t.expectString("", try reader.string());
    try t.expectEqual(0, try reader.int32());
    try t.expectString("", reader.rest());
}

test "Execute: write with name" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    const p = Execute{ .portal = "a name", .max_rows = 500 };
    try p.write(&w);

    var reader = Reader.init(w.buffered());
    try t.expectEqual('E', try reader.byte());
    try t.expectEqual(15, try reader.int32()); // payload length
    try t.expectString("a name", try reader.string());
    try t.expectEqual(500, try reader.int32());
    try t.expectString("", reader.rest());
}
