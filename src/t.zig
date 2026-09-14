const std = @import("std");

const Allocator = std.mem.Allocator;
const Conn = @import("conn.zig").Conn;

pub const allocator = std.testing.allocator;
pub const io = std.testing.io;

pub var arena = std.heap.ArenaAllocator.init(allocator);

pub fn reset() void {
    _ = arena.reset(.free_all);
}

// std.testing.expectEqual won't coerce expected to actual, which is a problem
// when expected is frequently a comptime.
// https://github.com/ziglang/zig/issues/4437
pub fn expectEqual(expected: anytype, actual: anytype) !void {
    try std.testing.expectEqual(@as(@TypeOf(actual), expected), actual);
}
pub fn expectDelta(expected: anytype, actual: anytype, delta: anytype) !void {
    expectEqual(true, expected - delta <= actual) catch |err| {
        std.debug.print("{d} !~ {d}", .{ expected, actual });
        return err;
    };
    expectEqual(true, expected + delta >= actual) catch |err| {
        std.debug.print("{d} !~ {d}", .{ expected, actual });
        return err;
    };
}
pub const expectError = std.testing.expectError;
pub const expectSlice = std.testing.expectEqualSlices;
pub const expectString = std.testing.expectEqualStrings;
pub fn expectStringSlice(expected: []const []const u8, actual: [][]const u8) !void {
    try expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| {
        try expectString(e, a);
    }
}

pub fn getRandom() std.Random.DefaultPrng {
    var seed: u64 = undefined;
    std.Io.random(io, std.mem.asBytes(&seed));
    return std.Random.DefaultPrng.init(seed);
}

pub fn setup() !void {
    var c = try connect(.{});
    defer c.deinit();
    _ = c.exec(
        \\ drop user if exists pgz_user_nopass;
        \\ drop user if exists pgz_user_clear;
        \\ drop user if exists pgz_user_scram_sha256;
        \\ drop user if exists pgz_user_ssl;
        \\ create user pgz_user_nopass;
        \\ create user pgz_user_clear with password 'pgz_user_clear_pw';
        \\ create user pgz_user_scram_sha256 with password 'pgz_user_scram_sha256_pw';
        \\ create user pgz_user_ssl with password 'pgz_user_ssl_pw';
    , .{}) catch |err| try fail(c, err);

    _ = c.exec(
        \\ drop table if exists simple_table;
        \\ create table simple_table (value text);
    , .{}) catch |err| try fail(c, err);

    _ = c.exec(
        \\ drop type if exists custom_enum cascade;
        \\ create type custom_enum as enum ('val1', 'val2');
    , .{}) catch |err| try fail(c, err);

    _ = c.exec(
        \\ drop table if exists all_types;
        \\ create table all_types (
        \\   id integer primary key,
        \\   col_int2 smallint,
        \\   col_int4 integer,
        \\   col_int8 bigint,
        \\   col_float4 float4,
        \\   col_float8 float8,
        \\   col_bool bool,
        \\   col_text text,
        \\   col_bytea bytea,
        \\   col_int2_arr smallint[],
        \\   col_int4_arr integer[],
        \\   col_int8_arr bigint[],
        \\   col_float4_arr float4[],
        \\   col_float8_arr float[],
        \\   col_bool_arr bool[],
        \\   col_text_arr text[],
        \\   col_bytea_arr bytea[],
        \\   col_enum custom_enum,
        \\   col_enum_arr custom_enum[],
        \\   col_uuid uuid,
        \\   col_uuid_arr uuid[],
        \\   col_numeric numeric,
        \\   col_numeric_arr numeric[],
        \\   col_timestamp timestamp,
        \\   col_timestamp_arr timestamp[],
        \\   col_json json,
        \\   col_json_arr json[],
        \\   col_jsonb jsonb,
        \\   col_jsonb_arr jsonb[],
        \\   col_char char,
        \\   col_char_arr char[],
        \\   col_charn char(3),
        \\   col_charn_arr char(2)[],
        \\   col_timestamptz timestamptz,
        \\   col_timestamptz_arr timestamptz[],
        \\   col_cidr cidr,
        \\   col_cidr_arr cidr[],
        \\   col_inet inet,
        \\   col_inet_arr inet[],
        \\   col_macaddr macaddr,
        \\   col_macaddr_arr macaddr[],
        \\   col_macaddr8 macaddr8,
        \\   col_macaddr8_arr macaddr8[]
        \\ );
    , .{}) catch |err| try fail(c, err);
}

// In-memory stand-in for lib.Stream: bytes queued with add() are served through
// an Io.Reader, split across reads however the buffer allows, so the reader
// tests see the same fragmentation a socket would produce.
pub const Stream = struct {
    interface: std.Io.Reader,
    _read_index: usize,
    _to_read: std.ArrayList(u8),

    pub fn init(buffer_size: usize) *Stream {
        const s = allocator.create(Stream) catch unreachable;
        s.* = .{
            ._read_index = 0,
            ._to_read = .empty,
            .interface = .{
                .vtable = &.{ .stream = stream },
                .buffer = allocator.alloc(u8, buffer_size) catch unreachable,
                .seek = 0,
                .end = 0,
            },
        };
        return s;
    }

    pub fn deinit(self: *Stream) void {
        allocator.free(self.interface.buffer);
        self._to_read.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn add(self: *Stream, value: []const u8) void {
        self._to_read.appendSlice(allocator, value) catch unreachable;
    }

    pub fn reader(self: *Stream) *std.Io.Reader {
        return &self.interface;
    }

    pub const ReadError = error{ReadFailed};

    pub fn getReadError(_: *Stream) ReadError {
        return error.ReadFailed;
    }

    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *Stream = @fieldParentPtr("interface", r);
        const pending = self._to_read.items[self._read_index..];
        if (pending.len == 0) {
            return error.EndOfStream;
        }
        const dest = limit.slice(try w.writableSliceGreedy(1));
        const n = @min(dest.len, pending.len);
        @memcpy(dest[0..n], pending[0..n]);
        self._read_index += n;
        w.advance(n);
        return n;
    }
};

pub fn connect(opts: anytype) !Conn {
    const T = @TypeOf(opts);

    var c = try Conn.open(io, allocator, .{
        .tls = if (@hasField(T, "tls")) opts.tls else .off,
        .host = if (@hasField(T, "host")) opts.host else "127.0.0.1",
        .read_buffer = if (@hasField(T, "read_buffer")) opts.read_buffer else 2000,
    });

    c.auth(authOpts(opts)) catch |err| {
        if (c.err) |pg| {
            @panic(pg.message);
        }
        @panic(@errorName(err));
    };
    return c;
}

pub fn authOpts(opts: anytype) Conn.AuthOpts {
    const T = @TypeOf(opts);
    return .{
        .database = if (@hasField(T, "database")) opts.database else "postgres",
        .username = if (@hasField(T, "username")) opts.username else "postgres",
        .password = if (@hasField(T, "password")) opts.password else "postgres",
    };
}

pub fn fail(c: Conn, err: anyerror) !void {
    if (c.err) |pg_err| {
        std.debug.print("PG ERROR: {s}\n", .{pg_err.message});
    }
    return err;
}

pub fn scalar(c: *Conn, sql: []const u8) i32 {
    var result = c.query(sql, .{}) catch unreachable;
    defer result.deinit();

    const row = (result.nextUnsafe() catch unreachable).?;
    const value = row.get(i32, 0);
    result.drain() catch unreachable;
    return value;
}
