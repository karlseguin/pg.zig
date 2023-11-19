const std = @import("std");

const ModuleMap = std.StringArrayHashMap(*std.Build.Module);
var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub fn build(b: *std.Build) !void {
	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	// setup our dependencies
	const allocator = gpa.allocator();

	var modules = ModuleMap.init(allocator);
	defer modules.deinit();
	const dep_opts = .{.target = target,.optimize = optimize};
	try modules.put("buffer", b.dependency("buffer", dep_opts).module("buffer"));

		// Expose this as a module that others can import
	_ = b.addModule("pg", .{
		.source_file = .{ .path = "src/pg.zig" },
		.dependencies = &.{ .{.name = "buffer", .module = modules.get("buffer").? }},
	});

	{
		// test step
		const lib_test = b.addTest(.{
			.root_source_file = .{ .path = "src/pg.zig" },
			.target = target,
			.optimize = optimize,
			.test_runner = "test_runner.zig",
		});
		addLibs(lib_test, modules);

		const run_test = b.addRunArtifact(lib_test);
		run_test.has_side_effects = true;

		const test_step = b.step("test", "Run unit tests");
		test_step.dependOn(&run_test.step);
	}
}

fn addLibs(step: *std.Build.CompileStep, modules: ModuleMap) void {
	var it = modules.iterator();
	while (it.next()) |m| {
		step.addModule(m.key_ptr.*, m.value_ptr.*);
	}
}
