const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
const Parse = @This();

prepared_statement: []const u8 = "",
sql: []const u8,

pub fn write(self: Parse, w: *Io.Writer) !void {
    // 4   + N     + 1 + S   + 1  + 2
    // len + $name + 0 + $sql + 0 + u16(0)
    const payload_len = 8 + self.prepared_statement.len + self.sql.len;

    try w.writeByte('P');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(self.prepared_statement);
    try w.writeByte(0);
    try w.writeAll(self.sql);
    try w.writeByte(0);
    // this is the # of parameters types that we plan on describing
    try w.writeAll(&.{ 0, 0 }); // 0 as a u16
}

const t = proto.testing;
const Reader = proto.Reader;
test "Parse: write no name" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    const p = Parse{ .sql = "select 1" };
    try p.write(&w);

    var reader = Reader.init(w.buffered());
    try t.expectEqual('P', try reader.byte());
    try t.expectEqual(16, try reader.int32()); // payload length
    try t.expectString("", try reader.string());
    try t.expectString("select 1", try reader.string());
    try t.expectEqual(0, try reader.int16());
    try t.expectString("", reader.rest());
}

test "Parse: write with name" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    const p = Parse{ .sql = "select 1", .prepared_statement = "a name" };
    try p.write(&w);

    var reader = Reader.init(w.buffered());
    try t.expectEqual('P', try reader.byte());
    try t.expectEqual(22, try reader.int32()); // payload length
    try t.expectString("a name", try reader.string());
    try t.expectString("select 1", try reader.string());
    try t.expectEqual(0, try reader.int16());
    try t.expectString("", reader.rest());
}
