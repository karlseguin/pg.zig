const std = @import("std");
const lib = @import("lib.zig");

const openssl = lib.openssl;

const posix = std.posix;

const Io = std.Io;
const Conn = lib.Conn;
const Allocator = std.mem.Allocator;

const DEFAULT_HOST = "127.0.0.1";

pub const Stream = if (lib.has_openssl) TLSStream else PlainStream;

// TODO - Looks like this is mostly a wrapper over the low level socket
// got a feeling that this could be replaced with Io.net.Stream for the most part

const TLSStream = struct {
    valid: bool,
    ssl: ?*openssl.SSL,
    socket: posix.socket_t,
    io: Io,

    pub fn connect(allocator: Allocator, io: Io, opts: Conn.Opts, ctx_: ?*openssl.SSL_CTX) !Stream {
        const plain = try PlainStream.connect(allocator, io, opts, null);
        errdefer plain.close();

        const socket = plain.socket;

        var ssl: ?*openssl.SSL = null;
        if (ctx_) |ctx| {
            // PostgreSQL TLS starts off as a plain connection which we upgrade
            try writeSocket(socket, io, &.{ 0, 0, 0, 8, 4, 210, 22, 47 });
            var buf = [1]u8{0};
            _ = try readSocket(socket, io, &buf);
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
                switch (opts.tls) {
                    .verify_full => openssl.SSL_set_verify(ssl, openssl.SSL_VERIFY_PEER, null),
                    else => {},
                }
            }

            if (openssl.SSL_set_fd(ssl, if (@import("builtin").os.tag == .windows) @intCast(@intFromPtr(socket)) else socket) != 1) {
                return error.SSLSetFdFailed;
            }

            {
                const ret = openssl.SSL_connect(ssl);
                if (ret != 1) {
                    const verification_code = openssl.SSL_get_verify_result(ssl);
                    if (comptime lib._stderr_tls) {
                        lib.printSSLError();
                    }
                    if (verification_code != openssl.X509_V_OK) {
                        if (comptime lib._stderr_tls) {
                            std.debug.print("ssl verification error: {s}\n", .{openssl.X509_verify_cert_error_string(verification_code)});
                        }
                        return error.SSLCertificationVerificationError;
                    }
                    return error.SSLConnectFailed;
                }
            }
        }

        return .{
            .ssl = ssl,
            .valid = true,
            .socket = socket,
            .io = io,
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
        return writeSocket(self.socket, self.io, data);
    }

    pub fn read(self: *Stream, buf: []u8) !usize {
        if (self.ssl) |ssl| {
            var read_len: usize = undefined;
            const result = openssl.SSL_read_ex(ssl, buf.ptr, @intCast(buf.len), &read_len);
            if (result <= 0) {
                self.valid = false;
                return error.SSLReadFailed;
            }
            return read_len;
        }

        return readSocket(self.socket, self.io, buf);
    }
};

const PlainStream = struct {
    socket: posix.socket_t,
    io: Io,

    pub fn connect(allocator: Allocator, io: Io, opts: Conn.Opts, _: anytype) !PlainStream {
        _ = allocator; // autofix
        const socket = blk: {
            const host = opts.host orelse DEFAULT_HOST;
            if (host.len > 0 and host[0] == '/') {
                if (comptime Io.net.has_unix_sockets == false or std.posix.AF == void) {
                    return error.UnixPathNotSupported;
                }
                const unix_address = try Io.net.UnixAddress.init(host);
                break :blk (try unix_address.connect(io)).socket.handle;
            }
            const port = opts.port orelse 5432;

            std.log.debug("Attempt to connect to {s}:{}", .{ host, port });
            const tcp_address = try Io.net.IpAddress.parse(host, port);
            break :blk (try tcp_address.connect(io, .{ .mode = .stream, .protocol = .tcp })).socket.handle;
        };
        errdefer posix.close(socket);

        return .{
            .socket = socket,
            .io = io,
        };
    }

    pub fn close(self: *const PlainStream) void {
        posix.close(self.socket);
    }

    pub fn writeAll(self: *const PlainStream, data: []const u8) !void {
        return writeSocket(self.socket, self.io, data);
    }

    pub fn read(self: *const PlainStream, buf: []u8) !usize {
        return readSocket(self.socket, self.io, buf);
    }
};

fn readSocket(socket: posix.socket_t, io: Io, buf: []u8) !usize {
    const stream: Io.net.Stream = .{ .socket = .{ .handle = socket, .address = undefined } };
    var vecs: [1][]u8 = .{buf};
    var reader = stream.reader(io, &.{});
    const r = &reader.interface;
    return try r.readVec(&vecs);
}

fn writeSocket(socket: posix.socket_t, io: Io, data: []const u8) !void {
    const stream: Io.net.Stream = .{ .socket = .{ .handle = socket, .address = undefined } };
    var buf: [1024]u8 = undefined;
    var writer = stream.writer(io, &buf);
    const w = &writer.interface;
    try w.writeAll(data);
    try w.flush();
}

fn isHostName(host: []const u8) bool {
    if (std.mem.indexOfScalar(u8, host, ':') != null) {
        // IPv6
        return false;
    }
    return std.mem.indexOfNone(u8, host, "0123456789.") != null;
}
