const std = @import("std");
const lib = @import("lib.zig");

const openssl = @cImport({
    @cInclude("openssl/ssl.h");
    @cInclude("openssl/err.h");
});

const posix = std.posix;

const Conn = lib.Conn;
const Allocator = std.mem.Allocator;

const DEFAULT_HOST = "127.0.0.1";

pub const Stream = if (lib.has_openssl) TLSStream else PlainStream;

const TLSStream = struct {
    valid: bool,
    ssl: ?*openssl.SSL,
    socket: posix.socket_t,

    pub fn connect(allocator: Allocator, opts: Conn.Opts, ctx_: ?*openssl.SSL_CTX) !Stream {
        const plain = try PlainStream.connect(allocator, opts, null);
        errdefer plain.close();

        const socket = plain.socket;

        var ssl: ?*openssl.SSL = null;
        if (ctx_) |ctx| {
            // PostgreSQL TLS starts off as a plain connection which we upgrade
            try writeSocket(socket, &.{0, 0, 0, 8, 4, 210, 22, 47});
            var buf = [1]u8{0};
            _ = try readSocket(socket, &buf);
            if (buf[0] != 'S') {
                return error.SSLNotSupportedByServer;
            }

            ssl = openssl.SSL_new(ctx) orelse return error.SSLNewFailed;
            errdefer openssl.SSL_free(ssl);

            if (opts.host) |host| {
                if (isHostName(host)) {
                    // don't send this for an ip address
                    var owned = false;
                    const h = opts._hostz orelse blk: {
                        owned = true;
                        break :blk try allocator.dupeZ(u8, host);
                    };

                    defer if (owned) {
                        allocator.free(h);
                    };

                    if (openssl.SSL_set_tlsext_host_name(ssl, h.ptr) != 1) {
                        return error.SSLHostNameFailed;
                    }
                }
            }

            if (openssl.SSL_set_fd(ssl, socket) != 1) {
                return error.SSLSetFdFailed;
            }

            {
                const ret = openssl.SSL_connect(ssl);
                if (ret != 1) {
                    lib.printSSLError();
                    return error.SSLConnectFailed;
                }
            }
        }

        return  .{
            .ssl = ssl,
            .valid = true,
            .socket = socket,
        };
    }

    pub fn close(self: *Stream) void {
        if (self.ssl) |ssl| {
            if (self.valid) {
                _ = openssl.SSL_shutdown(ssl);
                self.valid = false;
            }
            openssl.SSL_free(ssl);
        }
        posix.close(self.socket);
    }

    pub fn writeAll(self: *Stream, data: []const u8) !void {
        if (self.ssl) |ssl| {
            const result = openssl.SSL_write(ssl, data.ptr, @intCast(data.len));
            if (result <= 0) {
                self.valid = false;
                return error.SSLWriteFailed;
            }
            return;
        }
        return writeSocket(self.socket, data);
    }

    pub fn read(self: *Stream, buf: []u8) !usize {
        if (self.ssl) |ssl| {
            var read_len : usize = undefined;
            const result = openssl.SSL_read_ex(ssl, buf.ptr, @intCast(buf.len), &read_len);
            if (result <= 0) {
                self.valid = false;
                return error.SSLReadFailed;
            }
            return read_len;
        }

        return readSocket(self.socket, buf);
    }
};

const PlainStream = struct {
    socket: posix.socket_t,

    pub fn connect(allocator: Allocator, opts: Conn.Opts, _: anytype) !PlainStream {
        const socket = blk: {
            if (opts.unix_socket) |path| {
                if (comptime std.net.has_unix_sockets == false or std.posix.AF == void) {
                    return error.UnixPathNotSupported;
                }
                break :blk (try std.net.connectUnixSocket(path)).handle;
            } else {
                const host = opts.host orelse DEFAULT_HOST;
                const port = opts.port orelse 5432;
                break :blk (try std.net.tcpConnectToHost(allocator, host, port)).handle;
            }
        };
        errdefer posix.close(socket);

        return  .{
            .socket = socket,
        };
    }

    pub fn close(self: *const PlainStream) void {
        posix.close(self.socket);
    }

    pub fn writeAll(self: *const PlainStream, data: []const u8) !void {
        return writeSocket(self.socket, data);
    }

    pub fn read(self: *const PlainStream, buf: []u8) !usize {
        return readSocket(self.socket, buf);
    }
};

fn readSocket(socket: posix.socket_t, buf: []u8) !usize {
    return posix.read(socket, buf);
}

fn writeSocket(socket: posix.socket_t, data: []const u8) !void {
    var i: usize = 0;
    while (i < data.len) {
        i += try posix.write(socket, data[i..]);
    }
}

fn isHostName(host: []const u8) bool {
    if (std.mem.indexOfScalar(u8, host, ':') != null) {
        // IPv6
        return false;
    }
    return std.mem.indexOfNone(u8, host, "0123456789.") != null;
}
