const std = @import("std");
const lib = @import("lib.zig");
const openssl = lib.openssl;
const posix = std.posix;

const Allocator = std.mem.Allocator;
const Io = std.Io;

const DEFAULT_HOST = "127.0.0.1";

pub const Opts = struct {
    host: ?[]const u8 = null,
    port: ?u16 = null,
    reader_buffer: u16 = 1024,
    writer_buffer: u16 = 1024,
    tls: TLS = .off,
    _hostz: ?[:0]const u8 = null,

    pub const TLS = union(enum) {
        off: void,
        require: void,
        verify_full: ?[]const u8,
    };
};

pub const OpensslStream = struct {
    valid: bool,
    ssl: ?*openssl.SSL,
    ctx: ?*openssl.SSL_CTX,
    stream: Io.net.Stream,
    io: Io,

    pub const Error = error{
        SSLNotSupportedByServer,
        OpenSSLNotConfigured,
        SSLNewFailed,
        SSLContextNew,
        SSLMinVersion,
        SSLVerifyPaths,
        SSLDefaultVerifyPaths,
        SSLHostNameFailed,
        SSLSetFdFailed,
        SSLCertificationVerificationError,
        SSLConnectFailed,
    } || PlainStream.Error || Allocator.Error || Io.Writer.Error || Io.Reader.Error;

    pub const Reader = struct {
        io: Io,
        interface: Io.Reader,
        tlsstream: OpensslStream,
        err: ?Reader.Error,

        const max_iovecs_len = 2;

        pub const Error = error{
            SSLReadFailed,
        };

        pub fn init(stream: OpensslStream, io: Io, buffer: []u8) Reader {
            return .{
                .io = io,
                .interface = .{
                    .vtable = &.{
                        .stream = streamImpl,
                        .readVec = readVec,
                    },
                    .buffer = buffer,
                    .seek = 0,
                    .end = 0,
                },
                .tlsstream = stream,
                .err = null,
            };
        }

        fn streamImpl(io_r: *Io.Reader, io_w: *Io.Writer, limit: Io.Limit) Io.Reader.StreamError!usize {
            const dest = limit.slice(try io_w.writableSliceGreedy(1));
            var data: [1][]u8 = .{dest};
            const n = try readVec(io_r, &data);
            io_w.advance(n);
            return n;
        }

        fn readVec(io_r: *Io.Reader, data: [][]u8) Io.Reader.Error!usize {
            const r: *Reader = @alignCast(@fieldParentPtr("interface", io_r));
            var iovecs_buffer: [max_iovecs_len][]u8 = undefined;
            const dest_n, const data_size = try io_r.writableVector(&iovecs_buffer, data);
            const dest = iovecs_buffer[0..dest_n];
            std.debug.assert(dest[0].len > 0);

            var n: usize = undefined;
            const result = openssl.SSL_read_ex(r.tlsstream.ssl, dest[0].ptr, @intCast(dest[0].len), &n);
            if (result <= 0) {
                r.tlsstream.valid = false;
                r.err = error.SSLReadFailed;
                return error.ReadFailed;
            }
            if (n == 0) {
                return error.EndOfStream;
            }
            if (n > data_size) {
                r.interface.end += n - data_size;
                return data_size;
            }
            return n;
        }
    };

    pub const Writer = struct {
        io: Io,
        interface: Io.Writer,
        tlsstream: OpensslStream,
        err: ?Writer.Error = null,

        pub const Error = error{
            SSLWriteFailed,
        };

        pub fn init(stream: OpensslStream, io: Io, buffer: []u8) Writer {
            return .{
                .io = io,
                .tlsstream = stream,
                .interface = .{
                    .vtable = &.{
                        .drain = drain,
                    },
                    .buffer = buffer,
                },
            };
        }

        fn drain(io_w: *Io.Writer, data: []const []const u8, splat: usize) Io.Writer.Error!usize {
            const w: *Writer = @alignCast(@fieldParentPtr("interface", io_w));

            const buffered = io_w.buffered();

            var result: i32 = undefined;
            const ssl = w.tlsstream.ssl;

            if (buffered.len > 0) {
                result = openssl.SSL_write(ssl, buffered.ptr, @intCast(buffered.len));
                if (result <= 0) {
                    w.tlsstream.valid = false;
                    w.err = error.SSLWriteFailed;
                    return error.WriteFailed;
                }
                _ = io_w.consumeAll();
            }

            var n: usize = 0;
            for (data[0 .. data.len - 1]) |buf| {
                if (buf.len == 0) continue;
                result = openssl.SSL_write(ssl, buf.ptr, @intCast(buf.len));
                if (result <= 0) {
                    w.tlsstream.valid = false;
                    w.err = error.SSLWriteFailed;
                    return error.WriteFailed;
                }
                n += buf.len;
            }

            const pattern = data[data.len - 1];
            if (pattern.len > 0 and splat > 0) {
                for (0..splat) |_| {
                    result = openssl.SSL_write(ssl, pattern.ptr, @intCast(pattern.len));
                    if (result <= 0) {
                        w.tlsstream.valid = false;
                        w.err = error.SSLWriteFailed;
                        return error.WriteFailed;
                    }
                    n += pattern.len;
                }
            }

            return io_w.consume(n);
        }
    };

    pub fn reader(stream: OpensslStream, buffer: []u8) Reader {
        return .init(stream, stream.io, buffer);
    }

    pub fn writer(stream: OpensslStream, buffer: []u8) Writer {
        return .init(stream, stream.io, buffer);
    }

    pub fn connect(io: Io, allocator: Allocator, opts: Opts) Error!OpensslStream {
        const plain = try PlainStream.connect(io, opts);
        errdefer plain.close();

        const stream = plain.stream;

        var ssl_ctx: ?*openssl.SSL_CTX = null;
        switch (opts.tls) {
            .off => @panic("Not supported."),
            else => |tls_config| {
                if (comptime lib.has_openssl == false) {
                    return error.OpenSSLNotConfigured;
                }
                ssl_ctx = try lib.initializeSSLContext(tls_config);
            },
        }
        errdefer lib.freeSSLContext(ssl_ctx);

        var ssl: ?*openssl.SSL = null;
        if (ssl_ctx) |ctx| {
            // PostgreSQL TLS starts off as a plain connection which we upgrade
            var w = stream.writer(io, &.{});
            const w_io = &w.interface;
            try w_io.writeAll(&.{ 0, 0, 0, 8, 4, 210, 22, 47 });

            var buf = [1]u8{0};
            var r = stream.reader(io, &.{});
            const r_io = &r.interface;
            try r_io.readSliceAll(&buf);
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

            if (openssl.SSL_set_fd(ssl, if (@import("builtin").os.tag == .windows) @intCast(@intFromPtr(stream.socket.handle)) else stream.socket.handle) != 1) {
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
            .ctx = ssl_ctx,
            .valid = true,
            .stream = stream,
            .io = io,
        };
    }

    pub fn close(self: *OpensslStream) void {
        lib.freeSSLContext(self.ctx);
        if (self.ssl) |ssl| {
            if (self.valid) {
                _ = openssl.SSL_shutdown(ssl);
                self.valid = false;
            }
            openssl.SSL_free(ssl);
        }
        self.stream.close(self.io);
    }

    pub fn shutdown(self: *const OpensslStream, how: ShutdownHow) !void {
        return sockShutdown(self.stream.socket.handle, how);
    }
};

pub const TlsStream = struct {
    io: Io,
    allocator: Allocator,
    stream: Io.net.Stream,
    client: *std.crypto.tls.Client,
    output: Writer,

    pub const Error = error{
        SSLNotSupportedByServer,
    } || Allocator.Error || Io.RandomSecureError || Io.net.HostName.ValidateError || Io.net.HostName.ConnectError || Io.Reader.ShortError || Io.Writer.Error || std.crypto.tls.Client.InitError;

    pub const HostVerification = @FieldType(std.crypto.tls.Client.Options, "host");
    pub const CAVerification = @FieldType(std.crypto.tls.Client.Options, "ca");

    const Writer = struct {
        client: *std.crypto.tls.Client,
        interface: Io.Writer,

        pub fn init(c: *std.crypto.tls.Client) Writer {
            return .{
                .client = c,
                .interface = .{
                    .buffer = c.writer.buffer,
                    .vtable = &.{
                        .drain = drain,
                        .flush = flush,
                    },
                },
            };
        }

        fn drain(io_w: *Io.Writer, data: []const []const u8, splat: usize) Io.Writer.Error!usize {
            const w: *Writer = @alignCast(@fieldParentPtr("interface", io_w));
            w.client.writer.end = io_w.end;
            defer io_w.end = w.client.writer.end;

            return w.client.writer.vtable.drain(&w.client.writer, data, splat);
        }

        fn flush(io_w: *Io.Writer) Io.Writer.Error!void {
            const w: *Writer = @alignCast(@fieldParentPtr("interface", io_w));
            w.client.writer.end = io_w.end;
            defer io_w.end = w.client.writer.end;

            try w.client.writer.flush();
            return w.client.output.flush();
        }
    };

    pub fn reader(stream: *TlsStream) *Io.Reader {
        return &stream.client.reader;
    }

    pub fn writer(stream: *TlsStream) *Io.Writer {
        return &stream.output.interface;
    }

    pub fn connect(
        io: Io,
        allocator: Allocator,
        read_buffer: []u8,
        write_buffer: []u8,
        host_verification: HostVerification,
        ca_verification: CAVerification,
        opts: Opts,
    ) Error!TlsStream {
        const tls = std.crypto.tls;

        const host = opts.host orelse DEFAULT_HOST;
        const port = opts.port orelse 5432;
        const hostname: Io.net.HostName = try .init(host);
        var stream = try hostname.connect(io, port, .{ .mode = .stream });
        errdefer stream.close(io);

        const min_size = tls.Client.min_buffer_len;

        const rb = try allocator.alloc(u8, min_size);
        errdefer allocator.free(rb);
        const wb = try allocator.alloc(u8, min_size);
        errdefer allocator.free(wb);

        var plain_reader = try allocator.create(Io.net.Stream.Reader);
        errdefer allocator.destroy(plain_reader);
        plain_reader.* = stream.reader(io, rb);
        var plain_writer = try allocator.create(Io.net.Stream.Writer);
        errdefer allocator.destroy(plain_writer);
        plain_writer.* = stream.writer(io, wb);

        const pr = &plain_reader.interface;
        const pw = &plain_writer.interface;

        try pw.writeAll(&.{ 0, 0, 0, 8, 4, 210, 22, 47 });
        try pw.flush();

        var buf = [1]u8{0};
        _ = try pr.readSliceShort(&buf);
        if (buf[0] != 'S') {
            return error.SSLNotSupportedByServer;
        }

        var entropy: [tls.Client.Options.entropy_len]u8 = undefined;
        try std.Io.randomSecure(io, &entropy);

        const opt: tls.Client.Options = .{
            .host = host_verification,
            .ca = ca_verification,
            .read_buffer = read_buffer,
            .write_buffer = write_buffer,
            .entropy = &entropy,
            .realtime_now = .now(io, .real),
        };

        const client = try allocator.create(tls.Client);
        errdefer allocator.destroy(client);

        client.* = try tls.Client.init(pr, pw, opt);

        return .{
            .io = io,
            .allocator = allocator,
            .stream = stream,
            .client = client,
            .output = .init(client),
        };
    }

    pub fn close(self: *TlsStream) void {
        self.client.end() catch {};
        self.stream.close(self.io);
        const r: *Io.net.Stream.Reader = @fieldParentPtr("interface", self.client.input);
        const w: *Io.net.Stream.Writer = @fieldParentPtr("interface", self.client.output);
        self.allocator.free(self.client.input.buffer);
        self.allocator.free(self.client.output.buffer);
        self.allocator.destroy(r);
        self.allocator.destroy(w);
        self.allocator.destroy(self.client);
    }

    pub fn shutdown(self: *const TlsStream, how: ShutdownHow) !void {
        return sockShutdown(self.stream.socket.handle, how);
    }
};

pub const PlainStream = struct {
    io: Io,
    stream: Io.net.Stream,

    pub const Error = error{
        UnixPathNotSupported,
    } || Io.net.UnixAddress.ConnectError || Io.net.HostName.ValidateError || Io.net.HostName.ConnectError;

    pub fn reader(stream: *PlainStream, buffer: []u8) Io.net.Stream.Reader {
        return stream.stream.reader(stream.io, buffer);
    }

    pub fn writer(stream: *PlainStream, buffer: []u8) Io.net.Stream.Writer {
        return stream.stream.writer(stream.io, buffer);
    }

    pub fn connect(io: Io, opts: Opts) Error!PlainStream {
        const stream = try blk: {
            const host = opts.host orelse DEFAULT_HOST;
            if (host.len > 0 and host[0] == '/') {
                if (comptime Io.net.has_unix_sockets == false or std.posix.AF == void) {
                    return error.UnixPathNotSupported;
                }
                const addr: Io.net.UnixAddress = try .init(host);
                break :blk addr.connect(io);
            }
            const port = opts.port orelse 5432;
            const hostname: Io.net.HostName = try .init(host);
            break :blk hostname.connect(io, port, .{ .mode = .stream });
        };
        errdefer stream.close(io);

        return .{
            .io = io,
            .stream = stream,
        };
    }

    pub fn close(self: *const PlainStream) void {
        self.stream.close(self.io);
    }

    pub fn shutdown(self: *const PlainStream, how: ShutdownHow) !void {
        return sockShutdown(self.stream.socket.handle, how);
    }
};

const ShutdownHow = enum { recv, send, both };
fn sockShutdown(sock: posix.socket_t, how: ShutdownHow) !void {
    const native_os = @import("builtin").os.tag;
    if (native_os == .windows) {
        const windows = std.os.windows;
        const result = windows.ws2_32.shutdown(sock, switch (how) {
            .recv => windows.ws2_32.SD_RECEIVE,
            .send => windows.ws2_32.SD_SEND,
            .both => windows.ws2_32.SD_BOTH,
        });
        if (0 != result) switch (windows.ws2_32.WSAGetLastError()) {
            .WSAECONNABORTED => return error.ConnectionAborted,
            .WSAECONNRESET => return error.ConnectionResetByPeer,
            .WSAEINPROGRESS => return error.BlockingOperationInProgress,
            .WSAEINVAL => unreachable,
            .WSAENETDOWN => return error.NetworkSubsystemFailed,
            .WSAENOTCONN => return error.SocketNotConnected,
            .WSAENOTSOCK => unreachable,
            .WSANOTINITIALISED => unreachable,
            else => |err| return windows.unexpectedWSAError(err),
        };
    } else {
        const rc = posix.system.shutdown(sock, switch (how) {
            .recv => posix.system.SHUT.RD,
            .send => posix.system.SHUT.WR,
            .both => posix.system.SHUT.RDWR,
        });
        switch (posix.errno(rc)) {
            .SUCCESS => return,
            .BADF => unreachable,
            .INVAL => unreachable,
            .NOTCONN => return error.SocketNotConnected,
            .NOTSOCK => unreachable,
            .NOBUFS => return error.SystemResources,
            else => return error.Unexpected,
        }
    }
}

fn isHostName(host: []const u8) bool {
    if (std.mem.findScalar(u8, host, ':') != null) {
        // IPv6
        return false;
    }
    return std.mem.findNone(u8, host, "0123456789.") != null;
}
