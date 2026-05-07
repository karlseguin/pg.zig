const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
//Client sends this based on getting a request.sasl
const SASLInitialResponse = @This();

response: []const u8,
mechanism: []const u8,

pub fn write(self: SASLInitialResponse, w: *Io.Writer) !void {
    // 4 +   M          + 1 + 4             + R
    // len + $mechanism + 0 + $response.len + $response
    const payload_len = 9 + self.mechanism.len + self.response.len;

    // + 1 for the leading 'p'
    // const total_length = payload_len + 1;
    // try buf.ensureTotalCapacity(total_length);

    try w.writeByte('p');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(self.mechanism);
    try w.writeByte(0);
    try w.writeInt(u32, @intCast(self.response.len), .big);
    try w.writeAll(self.response);
    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "SASLInitialResponse: write" {
    var buf: Io.Writer.Allocating = .init(t.allocator);
    defer buf.deinit();

    const s = SASLInitialResponse{
        .mechanism = "SCRAM-SHA-256",
        .response = "a sasl response",
    };
    try s.write(&buf.writer);

    var reader = Reader.init(buf.written());
    try t.expectEqual('p', try reader.byte());
    try t.expectEqual(37, try reader.int32()); // payload length
    try t.expectString("SCRAM-SHA-256", try reader.string());
    try t.expectEqual(15, try reader.int32()); // length of response
    try t.expectString("a sasl response", reader.rest());
}
