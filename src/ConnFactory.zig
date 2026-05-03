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
    opts: Opts,
    err: ?PlainStream.Error,

    pub const Opts = struct {
        read_buffer: u16 = 1024,
        write_buffer: u16 = 1024,
        stream_opts: PlainStream.Opts = .{},
        conn_opts: Conn.Opts = .{},
    };

    pub fn init(
        io: Io,
        allocator: Allocator,
        opts: Opts,
    ) Plain {
        return .{
            .interface = .{
                .io = io,
                .allocator = allocator,
                .vtable = &.{
                    .create = Plain.create,
                    .destroy = Plain.destroy,
                },
            },
            .opts = opts,
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
        var f: *Plain = @alignCast(@fieldParentPtr("interface", cf));

        const io = cf.io;
        const allocator = cf.allocator;

        const item = try allocator.create(Item);
        errdefer allocator.destroy(item);
        item.stream = PlainStream.connect(io, f.opts.stream_opts) catch |err| {
            switch (err) {
                error.Canceled => return error.Canceled,
                else => |e| {
                    f.err = e;
                    return Error.CreateFailed;
                },
            }
        };
        errdefer item.stream.close();
        item.read_buffer = try allocator.alloc(u8, f.opts.read_buffer);
        errdefer allocator.free(item.read_buffer);
        item.reader = item.stream.reader(item.read_buffer);
        item.write_buffer = try allocator.alloc(u8, f.opts.write_buffer);
        errdefer allocator.free(item.write_buffer);
        item.writer = item.stream.writer(item.write_buffer);
        item.conn = try Conn.open(
            io,
            allocator,
            &item.reader.interface,
            &item.writer.interface,
            f.opts.conn_opts,
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
    opts: Opts,
    err: ?TlsStream.Error,

    pub const Opts = struct {
        read_buffer: u16 = 4096,
        write_buffer: u16 = 1024,
        stream_opts: TlsStream.Opts = .{},
        conn_opts: Conn.Opts = .{},
    };

    pub fn init(
        io: Io,
        allocator: Allocator,
        opts: Opts,
    ) Tls {
        return .{
            .interface = .{
                .io = io,
                .allocator = allocator,
                .vtable = &.{
                    .create = Tls.create,
                    .destroy = Tls.destroy,
                },
            },
            .opts = opts,
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
        var f: *Tls = @alignCast(@fieldParentPtr("interface", cf));

        const io = cf.io;
        const allocator = cf.allocator;

        const item = try allocator.create(Item);
        errdefer allocator.destroy(item);
        item.read_buffer = try allocator.alloc(u8, f.opts.read_buffer);
        errdefer allocator.free(item.read_buffer);
        item.write_buffer = try allocator.alloc(u8, f.opts.write_buffer);
        errdefer allocator.free(item.write_buffer);
        item.stream = TlsStream.connect(io, allocator, item.read_buffer, item.write_buffer, f.opts.stream_opts) catch |err| {
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
            f.opts.conn_opts,
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
    opts: Opts,
    err: ?OpensslStream.Error,

    pub const Opts = struct {
        read_buffer: u16 = 1024,
        write_buffer: u16 = 1024,
        stream_opts: OpensslStream.Opts = .{},
        conn_opts: Conn.Opts = .{},
    };

    pub fn init(
        io: Io,
        allocator: Allocator,
        opts: Opts,
    ) Openssl {
        return .{
            .interface = .{
                .io = io,
                .allocator = allocator,
                .vtable = &.{
                    .create = Openssl.create,
                    .destroy = Openssl.destroy,
                },
            },
            .opts = opts,
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
        var f: *Openssl = @alignCast(@fieldParentPtr("interface", cf));

        const io = cf.io;
        const allocator = cf.allocator;

        const item = try allocator.create(Item);
        errdefer allocator.destroy(item);
        item.stream = OpensslStream.connect(io, allocator, f.opts.stream_opts) catch |err| {
            switch (err) {
                error.Canceled => return error.Canceled,
                else => |e| {
                    f.err = e;
                    return Error.CreateFailed;
                },
            }
        };
        errdefer item.stream.close();
        item.read_buffer = try allocator.alloc(u8, f.opts.read_buffer);
        errdefer allocator.free(item.read_buffer);
        item.reader = item.stream.reader(item.read_buffer);
        item.write_buffer = try allocator.alloc(u8, f.opts.write_buffer);
        errdefer allocator.free(item.write_buffer);
        item.writer = item.stream.writer(item.write_buffer);
        item.conn = try Conn.open(
            io,
            allocator,
            &item.reader.interface,
            &item.writer.interface,
            f.opts.conn_opts,
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
