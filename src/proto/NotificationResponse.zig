const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
const Reader = proto.Reader;

const NotificationResponse = @This();

process_id: u32,
channel: []const u8,
payload: []const u8,

pub fn parse(data: []const u8) !NotificationResponse {
    var reader = Reader.init(data);
    return .{ .process_id = try reader.int32(), .channel = try reader.string(), .payload = try reader.string() };
}

const t = proto.testing;
test "NotificationResponse: parse" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    try w.writeInt(u32, 912, .big);
    try w.writeAll("chan-1");
    try w.writeByte(0);
    try w.writeAll("payload-2");
    try w.writeByte(0);

    const nr = try NotificationResponse.parse(w.buffered());
    try t.expectEqual(912, nr.process_id);
    try t.expectString("chan-1", nr.channel);
    try t.expectString("payload-2", nr.payload);
}
