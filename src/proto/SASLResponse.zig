const std = @import("std");
const proto = @import("_proto.zig");
const Reader = proto.Reader;
const Io = std.Io;

// #3 - Client finalizes with this
const SASLResponse = @This();

data: []const u8,

pub fn write(self: SASLResponse, w: *Io.Writer) !void {
    // 4 +   N
    // len + $data
    const payload_len = 4 + self.data.len;

    // + 1 for the leading 'p'
    // const total_length = payload_len + 1;
    // try buf.ensureTotalCapacity(total_length);

    // this nonsense is to skip the buffers bound checking, since we've already
    // ensured the available capacity
    // var w = buf.skip(total_length) catch unreachable;
    try w.writeByte('p');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(self.data);
    try w.flush();
}

const t = proto.testing;
test "SASLResponse: write" {
    var buf: Io.Writer.Allocating = .init(t.allocator);
    defer buf.deinit();

    const s = SASLResponse{
        .data = "the response",
    };
    try s.write(&buf.writer);

    var reader = Reader.init(buf.written());
    try t.expectEqual('p', try reader.byte());
    try t.expectEqual(16, try reader.int32()); // payload length
    try t.expectString("the response", reader.rest());
}
