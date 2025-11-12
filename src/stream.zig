const std = @import("std");
const lib = @import("lib.zig");

const Conn = lib.Conn;
const Io = std.Io;
const DEFAULT_HOST = "127.0.0.1";

pub const Stream = PlainStream;

const PlainStream = struct {
    io: Io,
    stream: std.Io.net.Stream,

    pub fn connect(io: Io, opts: Conn.Opts) !PlainStream {
        const host = opts.host orelse DEFAULT_HOST;
        const port = opts.port orelse 5432;
        const ip = try Io.net.IpAddress.parse(host, port);
        const stream = try ip.connect(io, .{ .mode = .stream, .protocol = .tcp }); // TODO: set timeout

        return .{
            .io = io,
            .stream = stream,
        };
    }

    pub fn close(self: *const PlainStream) void {
        self.stream.close(self.io);
    }

    pub fn writeAll(self: *const PlainStream, data: []const u8) !void {
        var buf: [1024]u8 = undefined;
        var writer = self.stream.writer(self.io, &buf);
        const w = &writer.interface;
        try w.writeAll(data);
        try w.flush();
    }

    pub fn read(self: *const PlainStream, data: []u8) !usize {
        var reader = self.stream.reader(self.io, &.{});
        const r = &reader.interface;
        var vecs: [1][]u8 = .{data};
        return r.readVec(&vecs);
    }
};
