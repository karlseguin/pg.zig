const std = @import("std");
const builtin = @import("builtin");
const lib = @import("lib.zig");
const tls = @import("tls");

const posix = std.posix;

const Conn = lib.Conn;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const DEFAULT_HOST = "127.0.0.1";

// A cheap handle over a connected socket; copies share one heap-allocated State.
// The State must never move: Conn/Listener are returned by value, and both the
// std.Io interfaces handed out by reader()/writer() and (under TLS) the
// tls.Connection's pointers to the ciphertext-side reader/writer live inside it.
pub const Stream = struct {
    io: Io,
    state: *State,

    pub const ReadError = tls.Connection.ReadError || Io.net.Stream.Reader.Error;
    pub const WriteError = tls.Connection.WriteError || Io.net.Stream.Writer.Error;

    const State = struct {
        allocator: Allocator,
        stream: Io.net.Stream,
        // Socket-side reader/writer. On a plaintext connection these are what
        // reader()/writer() hand out. Under TLS they carry ciphertext.
        tcp_reader: Io.net.Stream.Reader,
        tcp_writer: Io.net.Stream.Writer,
        // The message buffer: backs tcp_reader on a plaintext connection and
        // the plaintext-side tls reader otherwise.
        read_buf: []u8,
        tls: ?Tls,

        const Tls = struct {
            conn: tls.Connection,
            // plaintext side, i.e. what reader()/writer() hand out
            reader: tls.Connection.Reader,
            writer: tls.Connection.Writer,
            // record-sized ciphertext buffers, swapped into tcp_reader/tcp_writer
            input_buf: []u8,
            output_buf: []u8,
            rng: std.Random.IoSource,
        };

        fn deinit(self: *State) void {
            const allocator = self.allocator;
            if (self.tls) |*t| {
                allocator.free(t.output_buf);
                allocator.free(t.input_buf);
            }
            allocator.free(self.read_buf);
            allocator.destroy(self);
        }
    };

    pub fn connect(io: Io, allocator: Allocator, opts: Conn.Opts) !Stream {
        const socket = try connectSocket(io, opts);
        errdefer socket.close(io);

        const state = try allocator.create(State);
        state.* = .{
            .allocator = allocator,
            .stream = socket,
            .tcp_reader = undefined,
            .tcp_writer = undefined,
            .read_buf = &.{},
            .tls = null,
        };
        errdefer state.deinit();

        state.read_buf = try allocator.alloc(u8, @max(opts.read_buffer orelse 4096, 128));
        state.tcp_reader = socket.reader(io, state.read_buf);
        // Every write is a complete batch of messages, so the socket writer needs
        // no coalescing buffer of its own.
        state.tcp_writer = socket.writer(io, &.{});

        const self: Stream = .{ .io = io, .state = state };
        if (opts.tls != .off) {
            try self.startTls(allocator, opts);
        }
        return self;
    }

    fn startTls(self: Stream, allocator: Allocator, opts: Conn.Opts) !void {
        const io = self.io;
        const state = self.state;

        // PostgreSQL TLS starts off as a plain connection which we upgrade: send
        // the SSLRequest packet and require an 'S' (SSL supported) reply before
        // starting the TLS handshake.
        const w = &state.tcp_writer.interface;
        w.writeAll(&.{ 0, 0, 0, 8, 4, 210, 22, 47 }) catch return self.getWriteError();
        w.flush() catch return self.getWriteError();
        const reply = state.tcp_reader.interface.takeByte() catch |err| return switch (err) {
            error.EndOfStream => error.Closed,
            error.ReadFailed => self.getReadError(),
        };
        if (reply != 'S') {
            return error.SSLNotSupportedByServer;
        }

        // Host used for both SNI and certificate hostname verification. As with
        // the previous OpenSSL backend, we only use it for real hostnames; for an
        // IP address we pass an empty host, which suppresses SNI and the hostname
        // check (chain verification still runs for verify_full).
        const host = opts.host orelse DEFAULT_HOST;
        const tls_host: []const u8 = if (isHostName(host)) host else "";

        // only consulted during the handshake; the Connection does not retain it
        var root_ca: tls.config.cert.Bundle = .empty;
        defer root_ca.deinit(allocator);
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

        // once assigned, the caller's errdefer (State.deinit) owns the buffers
        state.tls = .{
            .conn = undefined,
            .reader = undefined,
            .writer = undefined,
            .input_buf = &.{},
            .output_buf = &.{},
            .rng = .{ .io = io },
        };
        const t = &state.tls.?;
        t.input_buf = try allocator.alloc(u8, tls.input_buffer_len);
        t.output_buf = try allocator.alloc(u8, tls.output_buffer_len);

        // A conforming server sends nothing after 'S' until it has our
        // ClientHello, so anything already buffered was injected by a
        // man-in-the-middle and must not be carried into the TLS reader
        // (CVE-2021-23214 in libpq).
        const r = &state.tcp_reader.interface;
        if (r.bufferedLen() != 0) {
            return error.UnexpectedDataAfterSSLResponse;
        }

        // The socket side now carries ciphertext and needs room for a full
        // record, so swap in the TLS buffers; the message buffer moves to the
        // plaintext side below.
        r.buffer = t.input_buf;
        r.seek = 0;
        r.end = 0;
        state.tcp_writer.interface.buffer = t.output_buf;

        // tls.client drives every byte of the handshake (and later every record)
        // through these std.Io Reader/Writer, so on the zio runtime the TLS I/O
        // is fully async and never blocks the executor.
        t.conn = tls.client(r, &state.tcp_writer.interface, .{
            .host = tls_host,
            .root_ca = root_ca,
            .insecure_skip_verify = skip_verify,
            .now = std.Io.Clock.real.now(io),
            .rng = t.rng.interface(),
        }) catch |err| return switch (err) {
            // tls.zig reports its transport failing as ReadFailed/WriteFailed;
            // surface the socket's error (e.g. Canceled) like a plaintext connect
            error.ReadFailed => state.tcp_reader.err orelse error.Unexpected,
            error.WriteFailed => state.tcp_writer.err orelse error.Unexpected,
            else => mapTlsError(err),
        };
        t.reader = t.conn.reader(state.read_buf);
        t.writer = t.conn.writer(&.{});
    }

    pub fn close(self: *Stream) void {
        const state = self.state;
        if (state.tls) |*t| {
            // Best-effort close_notify, shielded from cancellation like the
            // Terminate message so teardown can't be interrupted. OpenSSL's
            // SSL_shutdown was a blocking C call and couldn't be either.
            const prev = self.io.swapCancelProtection(.blocked);
            defer _ = self.io.swapCancelProtection(prev);
            t.conn.close() catch {};
        }
        state.stream.close(self.io);
        state.deinit();
    }

    pub fn shutdown(self: *const Stream, how: Io.net.ShutdownHow) !void {
        return self.state.stream.shutdown(self.io, how);
    }

    pub fn reader(self: Stream) *Io.Reader {
        const state = self.state;
        if (state.tls) |*t| return &t.reader.interface;
        return &state.tcp_reader.interface;
    }

    pub fn writer(self: Stream) *Io.Writer {
        const state = self.state;
        if (state.tls) |*t| return &t.writer.interface;
        return &state.tcp_writer.interface;
    }

    pub fn writeAll(self: *Stream, data: []const u8) WriteError!void {
        const w = self.writer();
        w.writeAll(data) catch return self.getWriteError();
        w.flush() catch return self.getWriteError();
    }

    // std.Io.Reader/Writer report every failure as ReadFailed/WriteFailed and
    // leave the real error on the concrete implementation; these recover it,
    // like std.http.Client.Connection.getReadError. Only meaningful right after
    // such a failure. Under TLS, a ReadFailed/WriteFailed recorded by tls.zig
    // means its own transport (our socket) failed, so look one level down.
    pub fn getReadError(self: Stream) ReadError {
        const state = self.state;
        if (state.tls) |*t| {
            if (t.reader.err) |err| {
                if (err != error.ReadFailed) return err;
            }
        }
        return state.tcp_reader.err orelse error.Unexpected;
    }

    pub fn getWriteError(self: Stream) WriteError {
        const state = self.state;
        if (state.tls) |*t| {
            if (t.writer.err) |err| {
                if (err != error.WriteFailed) return err;
            }
        }
        return state.tcp_writer.err orelse error.Unexpected;
    }
};

const TlsClientError = @typeInfo(@typeInfo(@TypeOf(tls.client)).@"fn".return_type.?).error_union.error_set;

// Map tls.zig handshake errors onto the error surface the previous OpenSSL
// backend exposed, so callers (and tests) see a stable set of errors.
// Certificate chain/hostname failures all surface as certificate errors from
// std.crypto, whose names start with "Certificate".
fn mapTlsError(err: TlsClientError) (TlsClientError || error{SSLCertificationVerificationError}) {
    if (std.mem.startsWith(u8, @errorName(err), "Certificate")) {
        return error.SSLCertificationVerificationError;
    }
    return err;
}

fn connectSocket(io: Io, opts: Conn.Opts) !Io.net.Stream {
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

    return stream;
}

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
