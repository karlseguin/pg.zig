const std = @import("std");
const builtin = @import("builtin");

const BORDER = "=" ** 80;

pub fn main() !void {
	var verbose = false;
	// var filter: ?[]const u8 = null;

	var slowest_time: usize = 0;
	var slowest_name: []const u8 = "";

	var pass: usize = 0;
	var fail: usize = 0;
	var skip: usize = 0;
	var leak: usize = 0;

	var timer = try std.time.Timer.start();
	for (builtin.test_functions) |t| {
		std.testing.allocator_instance = .{};
		var status = Status.pass;
		timer.reset();
		const result = t.func();
		const time = timer.lap() / 100_000;

		if (std.mem.eql(u8, "test_0", t.name)) {
			continue;
		}

		// strip out the test. prefix
		const friendly_name = t.name[5..];
		if (time > slowest_time) {
			slowest_time = time;
			slowest_name = friendly_name;
		}

		if (std.testing.allocator_instance.deinit() == .leak) {
			leak += 1;
			try print(.fail, "\n{s}\n\"{s}\" - Memory Leak\n{s}\n", .{BORDER, friendly_name, BORDER});
		}

		if (result) |_| {
			pass += 1;
		} else |err| switch (err) {
			error.SkipZigTest => {
				skip += 1;
				status = .skip;
			},
			else => {
				status = .fail;
				fail += 1;
				try print(.fail, "\n{s}\n\"{s}\" - {s}\n{s}\n", .{BORDER, friendly_name, @errorName(err), BORDER});
				if (@errorReturnTrace()) |trace| {
					std.debug.dumpStackTrace(trace.*);
				}
			}
		}

		if (verbose) {
			try print(status, "{s} ({d}ms)\n", .{friendly_name, time});
		} else {
			try print(status, ".", .{});
		}
	}

	const total_tests = pass + fail;
	const status = if (fail == 0) Status.pass else Status.fail;
	try print(status, "\n{d} of {d} test{s} passed\n", .{pass, total_tests, if (total_tests != 1) "s" else ""});
	if (skip > 0) {
		try print(.skip, "{d} test{s} skipped\n", .{skip, if (skip != 1) "s" else ""});
	}
	if (leak > 0) {
		try print(.fail, "{d} test{s} leaked\n", .{leak, if (leak != 1) "s" else ""});
	}
	std.os.exit(if (fail == 0) 0 else 1);
}

fn print(status: Status, comptime fmt: []const u8, args: anytype) !void {
	const color = switch (status) {
		.pass => "\x1b[32m",
		.fail => "\x1b[31m",
		else => "",
	};
	const out = std.io.getStdErr();
	try out.writeAll(color);
	try std.fmt.format(out.writer(), fmt, args);
	try out.writeAll("\x1b[0m");
}

const Status = enum {
	pass,
	fail,
	skip,
	text,
};
