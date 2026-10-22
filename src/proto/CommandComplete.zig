const std = @import("std");
const proto = @import("_proto.zig");
const Reader = proto.Reader;

const CommandComplete = @This();

tag: []const u8,

pub fn parse(data: []const u8) !CommandComplete {
    var reader = Reader.init(data);
    return .{
        .tag = try reader.restAsString(),
    };
}

// Finds the last number in the tag of the command completed. If no number is
// found, than 0 rows were affected. Commands like "create table" or "create
// role" have a tag that's just "CREATE XYZ".
// But update/delete/select/... have something like "delete #"
// "insert" is a bit more complicated, but the rows inserted is the last number
// so this works for it too.
pub fn rowsAffected(self: CommandComplete) ?i64 {
    const tag = self.tag;
    var start = tag.len;
    while (start > 0 and std.ascii.isDigit(tag[start - 1])) {
        start -= 1;
    }

    if (start == tag.len) {
        return null;
    }

    // only a server sending nonsense gets here with a number that doesn't fit
    return std.fmt.parseInt(i64, tag[start..], 10) catch null;
}

const t = proto.testing;
test "CommandComplete: parse" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    {
        // not a string (not null terminated)
        try buf.write("123");
        try t.expectError(error.NotAString, CommandComplete.parse(buf.string()));
    }

    {
        // success
        buf.reset();
        try buf.write("CREATE ROLE");
        try buf.writeByte(0);

        const c = try CommandComplete.parse(buf.string());
        try t.expectString("CREATE ROLE", c.tag);
    }
}

test "CommandComplete: rowsAffected" {
    {
        const c = CommandComplete{ .tag = "DROP ROLE" };
        try t.expectEqual(null, c.rowsAffected());
    }

    {
        const c = CommandComplete{ .tag = "INSERT 392 1" };
        try t.expectEqual(1, c.rowsAffected());
    }

    {
        const c = CommandComplete{ .tag = "DELETE 9392" };
        try t.expectEqual(9392, c.rowsAffected());
    }

    // tags only a broken or hostile server would send
    try t.expectEqual(null, (CommandComplete{ .tag = "" }).rowsAffected());
    try t.expectEqual(123, (CommandComplete{ .tag = "123" }).rowsAffected());
    try t.expectEqual(null, (CommandComplete{ .tag = "SELECT 99999999999999999999" }).rowsAffected());
}
