const std = @import("std");
const proto = @import("_proto.zig");
const Io = std.Io;

const Sync = @This();
pub fn write(_: Sync, w: *Io.Writer) !void {
    try w.writeAll(&.{ 'S', 0, 0, 0, 4 });
    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "Sync: write" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    const s = Sync{};
    try s.write(&w);

    var reader = Reader.init(w.buffered());
    try t.expectEqual('S', try reader.byte());
    try t.expectEqual(4, try reader.int32()); // payload length
    try t.expectString("", reader.rest());
}
