const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // setup our dependencies
    const dep_opts = .{ .target = target, .optimize = optimize };
    const modules = .{
        .buffer = b.dependency("buffer", dep_opts).module("buffer"),
        .metrics = b.dependency("metrics", dep_opts).module("metrics"),
    };

    // Expose this as a module that others can import
    const mod = b.addModule("pg", .{
        .root_source_file = b.path("src/pg.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "buffer", .module = modules.buffer },
            .{ .name = "metrics", .module = modules.metrics },
        },
    });

    {
        // test step
        const lib_test = b.addTest2(.{
            .root_module = mod,
            .test_runner = b.path("test_runner.zig"),
        });

        const run_test = b.addRunArtifact(lib_test);
        run_test.has_side_effects = true;

        const test_step = b.step("test", "Run unit tests");
        test_step.dependOn(&run_test.step);
    }
}
