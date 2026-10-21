const std = @import("std");
const proto = @import("_proto.zig");
const Reader = proto.Reader;

const BackendKeyData = @This();

// Protocol 3.0 keys are 4 bytes. 3.2 allows up to 256.
pub const max_key_len = 256;

pid: u32,
key: []const u8,

pub fn parse(data: []const u8) !BackendKeyData {
    var reader = Reader.init(data);
    const pid = try reader.int32();
    const key = reader.rest();
    if (key.len == 0 or key.len > max_key_len) {
        return error.InvalidBackendKeyData;
    }
    return .{
        .pid = pid,
        .key = key,
    };
}

const t = proto.testing;
test "BackendKeyData: parse" {
    try t.expectError(error.NoMoreData, BackendKeyData.parse(&.{ 0, 0, 1 }));
    try t.expectError(error.InvalidBackendKeyData, BackendKeyData.parse(&.{ 0, 0, 1, 2 }));

    {
        const kd = try BackendKeyData.parse(&.{ 0, 0, 1, 2, 9, 8, 7, 6 });
        try t.expectEqual(258, kd.pid);
        try t.expectSlice(u8, &.{ 9, 8, 7, 6 }, kd.key);
    }

    {
        // protocol 3.2 sends longer keys
        const data = [_]u8{ 0, 0, 0, 3 } ++ @as([32]u8, @splat(5));
        const kd = try BackendKeyData.parse(&data);
        try t.expectEqual(3, kd.pid);
        try t.expectEqual(32, kd.key.len);
    }
}
