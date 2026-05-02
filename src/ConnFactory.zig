const std = @import("std");
const lib = @import("lib.zig");
const stream = @import("stream.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Conn = lib.Conn;
const PlainStream = stream.PlainStream;
const OpensslStream = stream.OpensslStream;
const TlsStream = stream.TlsStream;

pub const Error = error{
    CreateFailed,
} || Allocator.Error || Io.Cancelable;

io: Io,
allocator: Allocator,
stream_opts: stream.Opts,
conn_opts: Conn.Opts,

vtable: *const VTable,

const ConnFactory = @This();

const VTable = struct {
    create: *const fn (*ConnFactory) Error!*Conn,
    destroy: *const fn (*const ConnFactory, *Conn) void,
};

pub fn create(factory: *ConnFactory) Error!*Conn {
    return factory.vtable.create(factory);
}

pub fn destroy(factory: *const ConnFactory, conn: *Conn) void {
    return factory.vtable.destroy(factory, conn);
}

pub const Plain = struct {
    interface: ConnFactory,
    err: ?PlainStream.Error,

    pub fn init(
        io: Io,
        allocator: Allocator,
        stream_opts: stream.Opts,
        conn_opts: Conn.Opts,
    ) Plain {
        return .{
            .interface = .{
                .io = io,
                .allocator = allocator,
                .stream_opts = stream_opts,
                .conn_opts = conn_opts,
                .vtable = &.{
                    .create = Plain.create,
                    .destroy = Plain.destroy,
                },
            },
            .err = null,
        };
    }

    const Item = struct {
        read_buffer: []u8,
        write_buffer: []u8,
        reader: Io.net.Stream.Reader,
        writer: Io.net.Stream.Writer,
        stream: PlainStream,
        conn: Conn,
    };

    pub fn create(cf: *ConnFactory) Error!*Conn {
        const io = cf.io;
        const allocator = cf.allocator;
        const stream_opts = cf.stream_opts;
        const conn_opts = cf.conn_opts;

        const item = try allocator.create(Item);
        errdefer allocator.destroy(item);
        item.stream = PlainStream.connect(io, stream_opts) catch |err| {
            var f: *Plain = @alignCast(@fieldParentPtr("interface", cf));
            switch (err) {
                error.Canceled => return error.Canceled,
                else => |e| {
                    f.err = e;
                    return Error.CreateFailed;
                },
            }
        };
        errdefer item.stream.close();
        item.read_buffer = try allocator.alloc(u8, stream_opts.reader_buffer);
        errdefer allocator.free(item.read_buffer);
        item.reader = item.stream.reader(item.read_buffer);
        item.write_buffer = try allocator.alloc(u8, stream_opts.writer_buffer);
        errdefer allocator.free(item.write_buffer);
        item.writer = item.stream.writer(item.write_buffer);
        item.conn = try Conn.open(
            io,
            allocator,
            &item.reader.interface,
            &item.writer.interface,
            conn_opts,
        );
        errdefer item.conn.deinit();
        return &item.conn;
    }

    pub fn destroy(f: *const ConnFactory, conn: *Conn) void {
        const allocator = f.allocator;
        var item: *Item = @alignCast(@fieldParentPtr("conn", conn));
        item.conn.deinit();
        item.stream.close();
        allocator.free(item.read_buffer);
        allocator.free(item.write_buffer);
        allocator.destroy(item);
    }
};

pub const Tls = struct {
    interface: ConnFactory,
    host_verification: TlsStream.HostVerification,
    ca_verification: TlsStream.CAVerification,
    err: ?TlsStream.Error,

    pub fn init(
        io: Io,
        allocator: Allocator,
        host_verification: TlsStream.HostVerification,
        ca_verification: TlsStream.CAVerification,
        stream_opts: stream.Opts,
        conn_opts: Conn.Opts,
    ) Tls {
        return .{
            .interface = .{
                .io = io,
                .allocator = allocator,
                .stream_opts = stream_opts,
                .conn_opts = conn_opts,
                .vtable = &.{
                    .create = Tls.create,
                    .destroy = Tls.destroy,
                },
            },
            .host_verification = host_verification,
            .ca_verification = ca_verification,
            .err = null,
        };
    }

    const Item = struct {
        read_buffer: []u8,
        write_buffer: []u8,
        stream: TlsStream,
        conn: Conn,
    };

    pub fn create(
        cf: *ConnFactory,
    ) Error!*Conn {
        const io = cf.io;
        const allocator = cf.allocator;
        const stream_opts = cf.stream_opts;
        const conn_opts = cf.conn_opts;

        var f: *Tls = @alignCast(@fieldParentPtr("interface", cf));

        const item = try allocator.create(Item);
        errdefer allocator.destroy(item);
        item.read_buffer = try allocator.alloc(u8, stream_opts.reader_buffer);
        errdefer allocator.free(item.read_buffer);
        item.write_buffer = try allocator.alloc(u8, stream_opts.writer_buffer);
        errdefer allocator.free(item.write_buffer);
        item.stream = TlsStream.connect(io, allocator, item.read_buffer, item.write_buffer, f.host_verification, f.ca_verification, stream_opts) catch |err| {
            switch (err) {
                error.Canceled => return error.Canceled,
                else => |e| {
                    f.err = e;
                    return Error.CreateFailed;
                },
            }
        };
        errdefer item.stream.close();
        item.conn = try Conn.open(
            io,
            allocator,
            item.stream.reader(),
            item.stream.writer(),
            conn_opts,
        );
        errdefer item.conn.deinit();
        return &item.conn;
    }

    pub fn destroy(f: *const ConnFactory, conn: *Conn) void {
        const allocator = f.allocator;
        var item: *Item = @alignCast(@fieldParentPtr("conn", conn));
        item.conn.deinit();
        item.stream.close();
        allocator.free(item.read_buffer);
        allocator.free(item.write_buffer);
        allocator.destroy(item);
    }
};

pub const Openssl = struct {
    interface: ConnFactory,
    err: ?OpensslStream.Error,

    pub fn init(
        io: Io,
        allocator: Allocator,
        stream_opts: stream.Opts,
        conn_opts: Conn.Opts,
    ) Openssl {
        return .{
            .interface = .{
                .io = io,
                .allocator = allocator,
                .stream_opts = stream_opts,
                .conn_opts = conn_opts,
                .vtable = &.{
                    .create = Openssl.create,
                    .destroy = Openssl.destroy,
                },
            },
            .err = null,
        };
    }

    const Item = struct {
        read_buffer: []u8,
        write_buffer: []u8,
        reader: OpensslStream.Reader,
        writer: OpensslStream.Writer,
        stream: OpensslStream,
        conn: Conn,
    };

    pub fn create(cf: *ConnFactory) Error!*Conn {
        const io = cf.io;
        const allocator = cf.allocator;
        const stream_opts = cf.stream_opts;
        const conn_opts = cf.conn_opts;
        std.debug.assert(stream_opts.tls != .off);

        const item = try allocator.create(Item);
        errdefer allocator.destroy(item);
        item.stream = OpensslStream.connect(io, allocator, stream_opts) catch |err| {
            var f: *Openssl = @alignCast(@fieldParentPtr("interface", cf));
            switch (err) {
                error.Canceled => return error.Canceled,
                else => |e| {
                    f.err = e;
                    return Error.CreateFailed;
                },
            }
        };
        errdefer item.stream.close();
        item.read_buffer = try allocator.alloc(u8, stream_opts.reader_buffer);
        errdefer allocator.free(item.read_buffer);
        item.reader = item.stream.reader(item.read_buffer);
        item.write_buffer = try allocator.alloc(u8, stream_opts.writer_buffer);
        errdefer allocator.free(item.write_buffer);
        item.writer = item.stream.writer(item.write_buffer);
        item.conn = try Conn.open(
            io,
            allocator,
            &item.reader.interface,
            &item.writer.interface,
            conn_opts,
        );
        errdefer item.conn.deinit();
        return &item.conn;
    }

    pub fn destroy(f: *const ConnFactory, conn: *Conn) void {
        const allocator = f.allocator;
        var item: *Item = @alignCast(@fieldParentPtr("conn", conn));
        item.conn.deinit();
        item.stream.close();
        allocator.free(item.read_buffer);
        allocator.free(item.write_buffer);
        allocator.destroy(item);
    }
};
