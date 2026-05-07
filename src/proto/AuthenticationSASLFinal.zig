const std = @import("std");
const proto = @import("_proto.zig");

const Io = std.Io;
const Reader = proto.Reader;

// #4 - Server finalizes with this
const AuthenticationSASLFinal = @This();

data: []const u8,

pub fn parse(data: []const u8) !AuthenticationSASLFinal {
    var reader = Reader.init(data);

    if (try reader.int32() != 12) {
        return error.NotSASLChallenge;
    }

    // can't really parse this, since it technically depends on the SASL
    // mechanism in use
    return .{
        .data = reader.rest(),
    };
}

const t = proto.testing;
test "AuthenticationSASLFinal: parse" {
    var buf: [128]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);

    {
        // too short
        try t.expectError(error.NoMoreData, AuthenticationSASLFinal.parse(&.{}));

        try w.writeAll("123");
        try t.expectError(error.NoMoreData, AuthenticationSASLFinal.parse(w.buffered()));
    }

    {
        // wrong special sasl type
        _ = w.consumeAll();
        try w.writeInt(u32, 13, .big);
        try t.expectError(error.NotSASLChallenge, AuthenticationSASLFinal.parse(w.buffered()));
    }

    {
        // success
        _ = w.consumeAll();
        try w.writeInt(u32, 12, .big);
        try w.writeAll("some server data");

        const final = try AuthenticationSASLFinal.parse(w.buffered());
        try t.expectString("some server data", final.data);
    }
}
