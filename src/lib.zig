// Exposed within this library
const std = @import("std");

const openssl = @cImport({
    @cInclude("openssl/ssl.h");
    @cInclude("openssl/err.h");
});

pub const log = std.log.scoped(.pg);

pub const types = @import("types.zig");
pub const proto = @import("proto.zig");
pub const auth = @import("auth.zig");
pub const Conn = @import("conn.zig").Conn;
pub const Stmt = @import("stmt.zig").Stmt;
pub const Pool = @import("pool.zig").Pool;
pub const Stream = @import("Stream.zig").Stream;
pub const metrics = @import("metrics.zig");
pub const has_openssl = @import("config").openssl;
pub const SSLCtx = if (has_openssl) openssl.SSL_CTX else void;

const result = @import("result.zig");
pub const Row = result.Row;
pub const Result = result.Result;
pub const Iterator = result.Iterator;
pub const QueryRow = result.QueryRow;
pub const Mapper = result.Mapper;

const reader = @import("reader.zig");
pub const Reader = reader.Reader;
pub const Message = reader.Message;

pub const testing = @import("t.zig");

const root = @import("root");
const _assert = blk: {
    if (@hasDecl(root, "pg_assert")) {
        break :blk root.pg_assert;
    }
    switch (@import("builtin").mode) {
        .ReleaseFast, .ReleaseSmall => break :blk false,
        else => break :blk true,
    }
};

pub fn assert(ok: bool) void {
    if (comptime _assert) {
        std.debug.assert(ok);
    }
}

pub fn assertDecodeType(comptime T: type, comptime expected_oids: []const i32, actual: i32) void {
    if (comptime _assert == false) {
        return;
    }

    inline for (expected_oids) |expected_oid| {
        if (expected_oid == actual) {
            return;
        }
    }

    log.warn("PostgreSQL value of type {s} cannot be read into a " ++ @typeName(T) ++ ". " ++
        "pg.zig has strict type checking when reading value.", .{types.oidToString(actual)});
    unreachable;
}

pub fn assertNotNull(comptime T: type, is_null: bool) void {
    if (comptime _assert == false) {
        return;
    }

    if (is_null == false) {
        return;
    }

    log.warn("PostgreSQL null column cannot be read into non-optional type (" ++ @typeName(T) ++ "). " ++
        "pg.zig has strict type checking when reading value.", .{});
    unreachable;
}

pub fn assertColumnName(name: []const u8, valid: bool) void {
    if (comptime _assert == false) {
        return;
    }

    if (valid) {
        return;
    }

    log.warn("Unknown column name '{s}'", .{name});
    unreachable;
}

pub const ParsedOpts = struct {
    opts: Pool.Opts,
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *ParsedOpts) void {
        self.arena.deinit();
    }
};

pub fn parseOpts(uri: std.Uri, allocator: std.mem.Allocator, size: u16, pool_timeout_ms: u32) !ParsedOpts {
    if (!std.mem.eql(u8, uri.scheme, "postgresql")) {
        return error.InvalidUriScheme;
    }

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();

    var tls = false;
    var tcp_user_timeout: ?u32 = null;
    if (uri.query) |qry| {
        const query_string = try qry.toRawMaybeAlloc(aa);
        var it = std.mem.splitScalar(u8, query_string, '&');
        while (it.next()) |param| {
            var it2 = std.mem.splitScalar(u8, param, '=');
            const key = it2.first();
            const val = it2.rest();
            if (std.mem.eql(u8, key, "tcp_user_timeout")) {
                tcp_user_timeout = try std.fmt.parseInt(u32, val, 10);
            } else if (std.mem.eql(u8, key, "sslmode")) {
                if (std.mem.eql(u8, val, "require")) {
                    tls = true;
                } else if (std.mem.eql(u8, val, "disable") == false) {
                    return error.UnsupportedSSLModeValue;
                }
            } else {
                return error.UnsupportedConnectionParam;
            }
        }
    }

    const path = std.mem.trimLeft(u8, try uri.path.toRawMaybeAlloc(aa), "/");
    return .{ .arena = arena, .opts = .{
        .size = size,
        .auth = .{
            .username = if (uri.user) |user| try user.toRawMaybeAlloc(aa) else "postgres",
            .password = if (uri.password) |password| try password.toRawMaybeAlloc(aa) else null,
            .database = if (path.len == 0) null else path,
            .timeout = tcp_user_timeout orelse 10_000,
        },
        .connect = .{
            .tls = tls,
            .port = uri.port orelse null,
            .host = if (uri.host) |host| try host.toRawMaybeAlloc(aa) else null,
        },
        .timeout = pool_timeout_ms,
    } };
}

pub fn initializeSSLContext() !*SSLCtx {
    // OpenSSL documentation says these are implicitly called, and only need to
    // be called if you're doing something special

    // if (openssl.OPENSSL_init_ssl(openssl.OPENSSL_INIT_LOAD_SSL_STRINGS | openssl.OPENSSL_INIT_LOAD_CRYPTO_STRINGS, null) != 1) {
    //     return error.OpenSSLInitSslFailed;
    // }

    // if (openssl.OPENSSL_init_crypto(openssl.OPENSSL_INIT_ADD_ALL_CIPHERS | openssl.OPENSSL_INIT_ADD_ALL_DIGESTS | openssl.OPENSSL_INIT_LOAD_CRYPTO_STRINGS, null) != 1) {
    //     return error.OpenSSLInitCryptoFailed;
    // }

    const ctx = openssl.SSL_CTX_new(openssl.TLS_client_method()) orelse {
        return error.SSLContextNew;
    };
    errdefer openssl.SSL_CTX_free(ctx);

    if (openssl.SSL_CTX_set_min_proto_version(ctx, openssl.TLS1_2_VERSION) != 1) {
        return error.SSLMinVersion;
    }

    _ = openssl.SSL_CTX_set_mode(ctx, openssl.SSL_MODE_AUTO_RETRY);

    if (openssl.SSL_CTX_set_default_verify_paths(ctx) != 1) {
        return error.SSLVerityPaths;
    }

    return ctx;
}

pub fn freeSSLContext(ctx: ?*SSLCtx) void {
    if (comptime has_openssl == false) {
        return;
    }

    if (ctx) |c| {
        openssl.SSL_CTX_free(c);
    }
}

pub fn printSSLError() void {
    if (comptime has_openssl == false) {
        return;
    }

    const bio = openssl.BIO_new(openssl.BIO_s_mem());
    defer _ = openssl.BIO_free(bio);
    openssl.ERR_print_errors(bio);
    var buf: [*]u8 = undefined;
    const len: usize = @intCast(openssl.BIO_get_mem_data(bio, &buf));
    if (len > 0) {
        std.debug.print("{s}\n", .{buf[0..len]});
    }
}

const TestCase = struct {
    uri: []const u8,
    expected_opts: Pool.Opts,
};

const valid_tcs: [2]TestCase = .{
    .{ .uri = "postgresql:///", .expected_opts = .{ .size = 10, .auth = .{ .username = "postgres" }, .connect = .{}, .timeout = 5000 } },
    .{ .uri = "postgresql://user:pass@somehost:1234/somedb?tcp_user_timeout=5678", .expected_opts = .{ .size = 10, .auth = .{
        .username = "user",
        .password = "pass",
        .database = "somedb",
        .timeout = 5678,
    }, .connect = .{
        .host = "somehost",
        .port = 1234,
    }, .timeout = 5000 } },
};

test "URI: parse valid" {
    const a = std.testing.allocator;
    for (valid_tcs) |tc| {
        var po = parseOpts(try std.Uri.parse(tc.uri), a, 10, 5000) catch |e| {
            std.log.err("failed to parse URI {s}", .{tc.uri});
            return e;
        };
        defer po.deinit();
        try std.testing.expectEqualDeep(tc.expected_opts, po.opts);
    }
}

test "URI: invalid scheme" {
    try std.testing.expectError(error.InvalidUriScheme, parseOpts(try std.Uri.parse("foobar:///"), std.testing.allocator, 0, 0));
}

test "URI: invalid params" {
    try std.testing.expectError(error.UnsupportedConnectionParam, parseOpts(try std.Uri.parse("postgresql:///?bar=baz"), std.testing.allocator, 0, 0));
}
