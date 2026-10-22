const std = @import("std");
const proto = @import("_proto.zig");

const CancelRequest = @This();

// The cancel request code, 1234 in the most significant 16 bits and 5678 in
// the least significant 16 bits.
const code = 80877102;

pid: u32,
key: []const u8,

pub fn write(self: CancelRequest, buf: *proto.Buffer) !void {
    const len = 12 + self.key.len;
    try buf.ensureTotalCapacity(len);
    var view = buf.skip(len) catch unreachable;
    view.writeIntBig(u32, @intCast(len));
    view.writeIntBig(u32, code);
    view.writeIntBig(u32, self.pid);
    view.write(self.key);
}

const t = proto.testing;
const Reader = proto.Reader;
test "CancelRequest: write" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    const c = CancelRequest{ .pid = 9001, .key = &.{ 1, 2, 3, 4 } };
    try c.write(&buf);

    var reader = Reader.init(buf.string());
    try t.expectEqual(16, try reader.int32());
    try t.expectEqual(80877102, try reader.int32());
    try t.expectEqual(9001, try reader.int32());
    try t.expectSlice(u8, &.{ 1, 2, 3, 4 }, reader.rest());
}
