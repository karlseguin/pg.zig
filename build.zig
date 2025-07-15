const std = @import("std");

const ModuleMap = std.StringArrayHashMap(*std.Build.Module);
var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // setup our dependencies
    const dep_opts = .{ .target = target, .optimize = optimize };
    const allocator = gpa.allocator();

    var modules = ModuleMap.init(allocator);
    defer modules.deinit();

    try modules.put("buffer", b.dependency("buffer", dep_opts).module("buffer"));
    try modules.put("metrics", b.dependency("metrics", dep_opts).module("metrics"));

    // Expose this as a module that others can import
    const pg_module = b.addModule("pg", .{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("src/pg.zig"),
        .imports = &.{ .{ .name = "buffer", .module = modules.get("buffer").? }, .{ .name = "metrics", .module = modules.get("metrics").? } },
    });

    var openssl = false;
    const openssl_lib_name = b.option([]const u8, "openssl_lib_name", "");
    const openssl_lib_path = b.option(std.Build.LazyPath, "openssl_lib_path", "");
    const openssl_include_path = b.option(std.Build.LazyPath, "openssl_include_path", "");

    if (openssl_include_path) |p| {
        openssl = true;
        pg_module.addIncludePath(p);
    }
    if (openssl_lib_path) |p| {
        openssl = true;
        pg_module.addLibraryPath(p);
    }
    if (openssl_lib_name != null) {
        openssl = true;
    }

    if (openssl) {
        pg_module.linkSystemLibrary("crypto", .{});
        pg_module.linkSystemLibrary(openssl_lib_name orelse "ssl", .{});
    }

    var column_names = false;
    const column_names_opt = b.option(bool, "column_names", "");

    if (column_names_opt) |val| {
        column_names = val;
    }

    {
        const options = b.addOptions();
        options.addOption(bool, "openssl", openssl);
        options.addOption(bool, "column_names", column_names);
        pg_module.addOptions("config", options);
    }

    {
        // test step
        const lib_test = b.addTest(.{
            .root_source_file = b.path("src/pg.zig"),
            .target = target,
            .optimize = optimize,
            .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
        });
        addLibs(lib_test, modules);
        lib_test.addLibraryPath(std.Build.LazyPath{ .cwd_relative = "/opt/openssl/lib" });
        lib_test.addIncludePath(std.Build.LazyPath{ .cwd_relative = "/opt/openssl/include" });
        lib_test.linkSystemLibrary("crypto");
        lib_test.linkSystemLibrary("ssl");

        {
            const options = b.addOptions();
            options.addOption(bool, "openssl", true);
            options.addOption(bool, "column_names", false);
            lib_test.root_module.addOptions("config", options);
        }

        const run_test = b.addRunArtifact(lib_test);
        run_test.has_side_effects = true;

        const test_step = b.step("test", "Run unit tests");
        test_step.dependOn(&run_test.step);
    }
}

fn addLibs(step: *std.Build.Step.Compile, modules: ModuleMap) void {
    var it = modules.iterator();
    while (it.next()) |m| {
        step.root_module.addImport(m.key_ptr.*, m.value_ptr.*);
    }
}
