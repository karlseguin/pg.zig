const std = @import("std");
const pg = @import("pg");

pub fn main() !void {
	var gpa = std.heap.GeneralPurposeAllocator(.{}){};
	const allocator = gpa.allocator();
	_ = try pg.Conn.connect(allocator, .{});
}
