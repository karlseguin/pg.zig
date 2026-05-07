const std = @import("std");
const proto = @import("_proto.zig");
const Reader = proto.Reader;
const Io = std.Io;

// #2 - Server responds with this
const AuthenticationSASLContinue = @This();

data: []const u8,

pub fn parse(data: []const u8) !AuthenticationSASLContinue {
    var reader = Reader.init(data);

    if (try reader.int32() != 11) {
        return error.NotSASLChallenge;
    }

    return .{
        .data = reader.rest(),
    };
}

const t = proto.testing;
test "AuthenticationSASLContinue: parse" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    {
        // too short
        try t.expectError(error.NoMoreData, AuthenticationSASLContinue.parse(&.{}));

        try w.writeAll("123");
        try t.expectError(error.NoMoreData, AuthenticationSASLContinue.parse(w.buffered()));
    }

    {
        // wrong special sasl type
        _ = w.consumeAll();
        try w.writeInt(u32, 12, .big);
        try t.expectError(error.NotSASLChallenge, AuthenticationSASLContinue.parse(w.buffered()));
    }

    {
        // success
        _ = w.consumeAll();
        try w.writeInt(u32, 11, .big);
        try w.writeAll("r=a-nounce,s=the-S@lt,i=4096");

        const c = try AuthenticationSASLContinue.parse(w.buffered());
        try t.expectString("r=a-nounce,s=the-S@lt,i=4096", c.data);
    }
}
