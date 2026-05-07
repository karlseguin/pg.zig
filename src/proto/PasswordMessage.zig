const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
const PasswordMessage = @This();

password: []const u8,

pub fn write(self: PasswordMessage, w: *Io.Writer) !void {
    // +4 since the payload length includes the length itself
    // +1 for null terminated string
    const payload_len = self.password.len + 5;

    // +1 for the type field, 'p'
    // const total_length = payload_len + 1;

    // try buf.ensureTotalCapacity(total_length);

    // var w = buf.skip(total_length) catch unreachable;
    try w.writeByte('p');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(self.password);
    try w.writeByte(0);
    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "PasswordMessage: write" {
    var buf: Io.Writer.Allocating = .init(t.allocator);
    defer buf.deinit();

    const pw = PasswordMessage{ .password = "gh@nim@" };
    try pw.write(&buf.writer);

    var reader = Reader.init(buf.written());
    try t.expectEqual('p', try reader.byte());
    try t.expectEqual(12, try reader.int32()); // payload length
    try t.expectString("gh@nim@", try reader.string());
}
