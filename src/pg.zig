const std = @import("std");
const lib = @import("lib.zig");

pub const Conn = lib.Conn;
pub const Pool = lib.Pool;

// pub const type_assert = true;

pub fn uuidToString(uuid: []const u8) ![36]u8 {
	return lib.types.UUID.toString(uuid);
}

const t = lib.testing;
test {
	try t.setup();
	std.testing.refAllDecls(@This());
}

test "pg: uuidToString" {
	try t.expectError(error.InvalidUUID, uuidToString(&.{73, 190, 142, 9, 170, 250, 176, 16, 73, 21}));

	const s = try uuidToString(&.{183, 204, 40, 47, 236, 67, 73, 190, 142, 9, 170, 250, 176, 16, 73, 21});
	try t.expectString("b7cc282f-ec43-49be-8e09-aafab0104915", &s);
}
