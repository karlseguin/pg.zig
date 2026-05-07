const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
const Query = @This();

sql: []const u8,

pub fn write(self: Query, w: *Io.Writer) !void {
    // 4   + S    + 1
    // len + $sql + 0
    const payload_len = 5 + self.sql.len;

    try w.writeByte('Q');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(self.sql);
    try w.writeByte(0);
    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "Query: write" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    const q = Query{ .sql = "select 1" };
    try q.write(&w);

    var reader = Reader.init(w.buffered());
    try t.expectEqual('Q', try reader.byte());
    try t.expectEqual(13, try reader.int32()); // payload length
    try t.expectString("select 1", try reader.restAsString());
}
