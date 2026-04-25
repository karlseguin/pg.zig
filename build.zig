const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // setup our dependencies
    const dep_opts = .{ .target = target, .optimize = optimize };

    const openssl_lib_name = b.option([]const u8, "openssl_lib_name", "");
    const openssl_lib_path = b.option(std.Build.LazyPath, "openssl_lib_path", "");
    const openssl_include_path = b.option(std.Build.LazyPath, "openssl_include_path", "");
    const openssl = b.option(bool, "openssl", "Enable OpenSSL/TLS support") orelse
        (openssl_lib_name != null or openssl_lib_path != null or openssl_include_path != null);

    // When openssl is not enabled, skip translate-c entirely and substitute
    // an empty stub module. Otherwise the build fails on hosts/targets where
    // <openssl/ssl.h> isn't reachable — e.g. when -Dtarget switches Zig into
    // cross-compile mode and stops searching the system include paths.
    const openssl_module = if (openssl) blk: {
        const Translator = @import("translate_c").Translator;
        const translate_c = b.dependency("translate_c", .{});
        const t: Translator = .init(translate_c, .{
            .c_source_file = b.path("src/openssl.h"),
            .target = target,
            .optimize = optimize,
        });
        if (openssl_include_path) |p| t.addIncludePath(p);
        break :blk t.mod;
    } else b.createModule(.{ .root_source_file = b.path("src/openssl_stub.zig") });

    // Expose this as a module that others can import
    const pg_module = b.addModule("pg", .{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("src/pg.zig"),
        .imports = &.{
            .{ .name = "buffer", .module = b.dependency("buffer", dep_opts).module("buffer") },
            .{ .name = "metrics", .module = b.dependency("metrics", dep_opts).module("metrics") },
            .{ .name = "openssl", .module = openssl_module },
        },
    });

    if (openssl) {
        if (openssl_lib_path) |p| pg_module.addLibraryPath(p);
        pg_module.linkSystemLibrary("crypto", .{});
        pg_module.linkSystemLibrary(openssl_lib_name orelse "ssl", .{});
        pg_module.link_libc = true;
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
        // test step — always built with openssl enabled
        const Translator = @import("translate_c").Translator;
        const translate_c = b.dependency("translate_c", .{});
        const t: Translator = .init(translate_c, .{
            .c_source_file = b.path("src/openssl.h"),
            .target = target,
            .optimize = optimize,
        });
        if (openssl_include_path) |p| t.addIncludePath(p);

        const lib_test = b.addTest(.{
            .root_module = b.createModule(.{
                .target = target,
                .optimize = optimize,
                .root_source_file = b.path("src/pg.zig"),
                .imports = &.{
                    .{ .name = "buffer", .module = b.dependency("buffer", dep_opts).module("buffer") },
                    .{ .name = "metrics", .module = b.dependency("metrics", dep_opts).module("metrics") },
                    .{ .name = "openssl", .module = t.mod },
                },
            }),
            .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
        });
        if (openssl_lib_path) |p|
            lib_test.root_module.addLibraryPath(p);
        lib_test.root_module.linkSystemLibrary("crypto", .{});
        lib_test.root_module.linkSystemLibrary("ssl", .{});

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
