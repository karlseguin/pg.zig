const std = @import("std");
const builtin = @import("builtin");
const lib = @import("lib.zig");
const tls = @import("tls");

const posix = std.posix;

const Conn = lib.Conn;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const DEFAULT_HOST = "127.0.0.1";

pub const Stream = TLSStream;

// Per-connection TLS state, heap-allocated so it lives at a stable address.
// tls.Connection stores pointers to `tcp_reader.interface`/`tcp_writer.interface`
// (the ciphertext side), so this struct must never be moved after `tls.client`.
// Both the Conn's Stream copy and the Reader's Stream copy hold the same
// `*TlsState` pointer, so reads and writes funnel through one shared connection.
const TlsState = struct {
    conn: tls.Connection,
    tcp_reader: Io.net.Stream.Reader,
    tcp_writer: Io.net.Stream.Writer,
    rng: std.Random.IoSource,
    // ciphertext-side buffers backing tcp_reader/tcp_writer
    read_buf: []u8,
    write_buf: []u8,
    allocator: Allocator,

    fn deinit(self: *TlsState) void {
        const allocator = self.allocator;
        allocator.free(self.write_buf);
        allocator.free(self.read_buf);
        allocator.destroy(self);
    }
};

const TLSStream = struct {
    io: Io,
    stream: Io.net.Stream,
    // null for a plaintext (tls = .off) connection.
    tls: ?*TlsState,
    valid: bool,

    pub fn connect(io: Io, allocator: Allocator, opts: Conn.Opts) !Stream {
        const plain = try PlainStream.connect(io, allocator, opts);
        errdefer plain.close();

        const stream = plain.stream;

        if (opts.tls == .off) {
            return .{ .io = io, .stream = stream, .tls = null, .valid = true };
        }

        // PostgreSQL TLS starts off as a plain connection which we upgrade: send
        // the SSLRequest packet and require an 'S' (SSL supported) reply before
        // starting the TLS handshake.
        try writeStream(stream, io, &.{ 0, 0, 0, 8, 4, 210, 22, 47 });
        var buf = [1]u8{0};
        _ = try readStream(stream, io, &buf);
        if (buf[0] != 'S') {
            return error.SSLNotSupportedByServer;
        }

        // Host used for both SNI and certificate hostname verification. As with
        // the previous OpenSSL backend, we only use it for real hostnames; for an
        // IP address we pass an empty host, which suppresses SNI and the hostname
        // check (chain verification still runs for verify_full).
        const host = opts.host orelse DEFAULT_HOST;
        const tls_host: []const u8 = if (isHostName(host)) host else "";

        var root_ca: tls.config.cert.Bundle = .empty;
        errdefer root_ca.deinit(allocator);
        const skip_verify = switch (opts.tls) {
            .off => unreachable,
            .require => true,
            .verify_full => |ca_path| blk: {
                root_ca = if (ca_path) |p|
                    try tls.config.cert.fromFilePath(allocator, io, .cwd(), p)
                else
                    try tls.config.cert.fromSystem(allocator, io);
                break :blk false;
            },
        };

        const state = try allocator.create(TlsState);
        errdefer allocator.destroy(state);
        state.allocator = allocator;
        state.rng = .{ .io = io };

        state.read_buf = try allocator.alloc(u8, tls.input_buffer_len);
        errdefer allocator.free(state.read_buf);
        state.write_buf = try allocator.alloc(u8, tls.output_buffer_len);
        errdefer allocator.free(state.write_buf);

        state.tcp_reader = stream.reader(io, state.read_buf);
        state.tcp_writer = stream.writer(io, state.write_buf);

        // tls.client drives every byte of the handshake (and later every record)
        // through these std.Io Reader/Writer, so on the zio runtime the TLS I/O
        // is fully async and never blocks the executor.
        state.conn = tls.client(&state.tcp_reader.interface, &state.tcp_writer.interface, .{
            .host = tls_host,
            .root_ca = root_ca,
            .insecure_skip_verify = skip_verify,
            .now = std.Io.Clock.real.now(io),
            .rng = state.rng.interface(),
        }) catch |err| return mapTlsError(err);

        // root_ca is only consulted during the handshake; the Connection does
        // not retain it.
        root_ca.deinit(allocator);
        root_ca = .empty;

        return .{ .io = io, .stream = stream, .tls = state, .valid = true };
    }

    pub fn close(self: *Stream) void {
        if (self.tls) |state| {
            if (self.valid) {
                // Best-effort close_notify, shielded from cancellation like the
                // Terminate message so teardown can't be interrupted. OpenSSL's
                // SSL_shutdown was a blocking C call and couldn't be either.
                const prev = self.io.swapCancelProtection(.blocked);
                defer _ = self.io.swapCancelProtection(prev);
                state.conn.close() catch {};
                self.valid = false;
            }
            state.deinit();
            self.tls = null;
        }
        self.stream.close(self.io);
    }

    pub fn shutdown(self: *const Stream, how: Io.net.ShutdownHow) !void {
        return self.stream.shutdown(self.io, how);
    }

    pub fn writeAll(self: *Stream, data: []const u8) !void {
        if (self.tls) |state| {
            state.conn.writeAll(data) catch |err| {
                self.valid = false;
                return err;
            };
            return;
        }
        return writeStream(self.stream, self.io, data);
    }

    pub fn read(self: *Stream, buf: []u8) !usize {
        if (self.tls) |state| {
            return state.conn.read(buf) catch |err| {
                self.valid = false;
                return err;
            };
        }

        return readStream(self.stream, self.io, buf);
    }
};

// Map tls.zig handshake errors onto the error surface the previous OpenSSL
// backend exposed, so callers (and tests) see a stable set of errors.
// Certificate chain/hostname failures all surface as certificate errors from
// std.crypto, whose names start with "Certificate".
fn mapTlsError(err: anyerror) anyerror {
    if (std.mem.startsWith(u8, @errorName(err), "Certificate")) {
        return error.SSLCertificationVerificationError;
    }
    return err;
}

const PlainStream = struct {
    io: Io,
    stream: Io.net.Stream,

    pub fn connect(io: Io, _: Allocator, opts: Conn.Opts) !PlainStream {
        const host = opts.host orelse DEFAULT_HOST;
        const is_unix = host.len > 0 and host[0] == '/';

        const stream = try blk: {
            if (is_unix) {
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

        if (is_unix == false) {
            try setKeepalive(stream.socket.handle, opts);
        }

        return .{
            .io = io,
            .stream = stream,
        };
    }

    pub fn close(self: *const PlainStream) void {
        self.stream.close(self.io);
    }

    pub fn shutdown(self: *const PlainStream, how: Io.net.ShutdownHow) !void {
        return self.stream.shutdown(self.io, how);
    }

    pub fn writeAll(self: *const PlainStream, data: []const u8) !void {
        return writeStream(self.stream, self.io, data);
    }

    pub fn read(self: *const PlainStream, buf: []u8) !usize {
        return readStream(self.stream, self.io, buf);
    }
};

const TCP = switch (builtin.os.tag) {
    // Zig doesn't expose these /shrug
    .freebsd, .dragonfly => struct {
        pub const KEEPIDLE = 256;
        pub const KEEPINTVL = 512;
        pub const KEEPCNT = 1024;
    },
    .netbsd => struct {
        pub const KEEPIDLE = 3;
        pub const KEEPINTVL = 5;
        pub const KEEPCNT = 6;
    },
    .illumos => struct {
        pub const KEEPIDLE = 0x22;
        pub const KEEPCNT = 0x23;
        pub const KEEPINTVL = 0x24;
    },
    else => if (posix.TCP == void) struct {} else posix.TCP,
};

fn setKeepalive(handle: posix.socket_t, opts: Conn.Opts) !void {
    if (opts.keepalive == false) {
        return;
    }
    const on: c_int = 1;
    try setsockopt(handle, posix.SOL.SOCKET, posix.SO.KEEPALIVE, std.mem.asBytes(&on));

    const level = posix.IPPROTO.TCP;

    if (opts.keepalive_idle) |idle| {
        const optname: ?u32 = comptime if (@hasDecl(TCP, "KEEPIDLE"))
            TCP.KEEPIDLE
        else if (@hasDecl(TCP, "KEEPALIVE"))
            TCP.KEEPALIVE
        else
            null;
        if (optname) |name| {
            const v: c_int = @intCast(idle);
            setsockopt(handle, level, name, std.mem.asBytes(&v)) catch {};
        }
    }

    if (opts.keepalive_interval) |intvl| {
        if (comptime @hasDecl(TCP, "KEEPINTVL")) {
            const v: c_int = @intCast(intvl);
            setsockopt(handle, level, TCP.KEEPINTVL, std.mem.asBytes(&v)) catch {};
        }
    }

    if (opts.keepalive_count) |cnt| {
        if (comptime @hasDecl(TCP, "KEEPCNT")) {
            const v: c_int = @intCast(cnt);
            setsockopt(handle, level, TCP.KEEPCNT, std.mem.asBytes(&v)) catch {};
        }
    }
}

fn setsockopt(fd: posix.socket_t, level: i32, optname: u32, opt: []const u8) !void {
    if (@import("builtin").os.tag != .windows) {
        return posix.setsockopt(fd, level, optname, opt);
    }

    const SO = posix.SO;
    const SOL = posix.SOL;
    const timeval = posix.timeval;

    var ms_buf: u32 = 0;
    var opt_ptr: [*]const u8 = opt.ptr;
    var opt_len: i32 = @intCast(opt.len);
    if (level == SOL.SOCKET and (optname == SO.RCVTIMEO or optname == SO.SNDTIMEO) and opt.len == @sizeOf(timeval)) {
        const tv: *const timeval = @ptrCast(@alignCast(opt.ptr));
        const total_ms = @as(i64, tv.sec) * 1000 + @divTrunc(@as(i64, tv.usec), 1000);
        ms_buf = if (total_ms < 0) 0 else @intCast(@min(total_ms, std.math.maxInt(u32)));
        opt_ptr = @ptrCast(&ms_buf);
        opt_len = @sizeOf(u32);
    }

    const in: []const u8 = @ptrCast(&std.os.windows.AFD.SOCKOPT_INFO{
        .mode = .set,
        .level = level,
        .optname = optname,
        .optval = opt_ptr,
        .optlen = @intCast(opt_len),
    });

    var iosb: std.os.windows.IO_STATUS_BLOCK = undefined;
    switch (std.os.windows.ntdll.NtDeviceIoControlFile(
        fd,
        null, // event
        null, // APC routine
        null, // APC context
        &iosb,
        std.os.windows.IOCTL.AFD.SOCKOPT,
        if (in.len > 0) in.ptr else null,
        @intCast(in.len),
        null,
        0,
    )) {
        .SUCCESS => return,
        .CANCELLED => return error.Canceled,
        .INSUFFICIENT_RESOURCES => return error.SystemResources,
        else => |status| return std.os.windows.unexpectedStatus(status),
    }
}

fn readStream(stream: Io.net.Stream, io: Io, buf: []u8) !usize {
    var vecs: [1][]u8 = .{buf};
    var reader = stream.reader(io, &.{});
    const r = &reader.interface;
    return r.readVec(&vecs) catch |err| switch (err) {
        error.ReadFailed => return reader.err orelse err,
        else => return err,
    };
}

fn writeStream(stream: Io.net.Stream, io: Io, data: []const u8) !void {
    var buf: [1024]u8 = undefined;
    var writer = stream.writer(io, &buf);
    const w = &writer.interface;
    w.writeAll(data) catch |err| switch (err) {
        error.WriteFailed => return writer.err orelse err,
    };
    w.flush() catch |err| switch (err) {
        error.WriteFailed => return writer.err orelse err,
    };
}

// Sends a best-effort Terminate ('X') message, shielded from cancellation so
// teardown can't be interrupted.
pub fn sendTerminate(stream: *Stream, io: Io) void {
    const prev = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(prev);
    stream.writeAll(&.{ 'X', 0, 0, 0, 4 }) catch {};
}

fn isHostName(host: []const u8) bool {
    if (std.mem.findScalar(u8, host, ':') != null) {
        // IPv6
        return false;
    }
    return std.mem.findNone(u8, host, "0123456789.") != null;
}

const windows = @import("windows.zig");
