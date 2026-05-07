const std = @import("std");
const lib = @import("lib.zig");

const proto = lib.proto;
const Conn = lib.Conn;
const Reader = lib.Reader;
const NotificationResponse = lib.proto.NotificationResponse;

const Allocator = std.mem.Allocator;
const Io = std.Io;

const ListenError = union(enum) {
    err: anyerror,
    pg: lib.proto.Error,
};

pub const Listener = struct {
    err: ?ListenError = null,
    closed: bool = false,

    // Used to read data from PG. Has its own buffer which can grow dynamically
    _pgreader: Reader,

    _writer: *Io.Writer,

    // If we get a PG error, we'll return a LIstenError.pg, and we'll own its
    // memory.
    _err_data: ?[]const u8 = null,

    _allocator: Allocator,

    _io: Io,

    pub fn open(io: Io, allocator: Allocator, reader: *Io.Reader, writer: *Io.Writer, opts: Conn.Opts) !Listener {
        const pgreader = try Reader.init(allocator, opts.read_buffer, reader);
        errdefer pgreader.deinit();

        return .{
            ._pgreader = pgreader,
            ._writer = writer,
            ._allocator = allocator,
            ._io = io,
        };
    }

    pub fn deinit(self: *Listener) void {
        self.stop() catch {};

        if (self._err_data) |err_data| {
            self._allocator.free(err_data);
        }

        self._pgreader.deinit();
    }

    pub fn stop(self: *Listener) !void {
        if (@atomicRmw(bool, &self.closed, .Xchg, true, .monotonic) == true) {
            return;
        }

        // try to send a Terminate to the DB
        const w = self._writer;
        try w.writeAll(&.{ 'X', 0, 0, 0, 4 });
        try w.flush();

        // return self._stream.shutdown(.both);
    }

    pub fn auth(self: *Listener, opts: Conn.AuthOpts) !void {
        const w = self._writer;
        if (try lib.auth.auth(self._io, w, &self._pgreader, opts)) |raw_pg_err| {
            return self.setErr(raw_pg_err);
        }

        while (true) {
            const msg = try self.read();
            switch (msg.type) {
                'Z' => return,
                'K' => {}, // TODO: BackendKeyData
                'S' => {}, // TODO: ParameterStatus,
                else => return error.UnexpectedDBMessage,
            }
        }
    }

    const ListenOpts = struct {
        timeout: u32 = 0,
    };
    pub fn listen(self: *Listener, channel: []const u8, opts: ListenOpts) !void {
        // LISTEN doesn't support parameterized queries. It has to be a simple query.
        // We don't use proto.Query because we want to quote the identifier.

        const w = self._writer;
        try w.writeByte('Q');

        // "LISTEN " = 7
        // "IDENTIFIER" = 128
        // max identifier size is 63, but if we need to quote every character, that's
        // 126. + 2 for the opening and closing quote
        // + 1 for null terminator
        var buf: [136]u8 = undefined;
        var b: Io.Writer = .fixed(&buf);

        try b.writeAll("LISTEN \"");

        // + 4 for the length itself
        // + 7 for the LISTEN
        // + 2 for the quotes
        // + 1 for the null terminator
        var len = 11 + channel.len + 3;
        for (channel) |c| {
            if (c == '"') {
                len += 1;
                try b.writeAll("\"\"");
            } else {
                try b.writeByte(c);
            }
        }
        try b.writeByte('"');
        try b.writeByte(0);

        const content = b.buffered();

        // fill in the length
        try w.writeInt(u32, @intCast(len), .big);

        try w.writeAll(content);
        try w.flush();

        {
            // we expect a command complete ('C')
            const msg = try self.read();
            switch (msg.type) {
                'C' => {},
                else => return error.UnexpectedDBMessage,
            }
        }

        {
            // followed by a ReadyForQuery ('Z')
            const msg = try self.read();
            switch (msg.type) {
                'Z' => {},
                else => return error.UnexpectedDBMessage,
            }
        }

        try self._pgreader.startFlow(null, opts.timeout);
    }

    pub fn next(self: *Listener) ?NotificationResponse {
        if (@atomicLoad(bool, &self.closed, .acquire) == true) {
            return null;
        }

        const msg = self.read() catch |err| {
            self.err = .{ .err = err };
            return null;
        };

        switch (msg.type) {
            'A' => return NotificationResponse.parse(msg.data) catch |err| {
                self.err = .{ .err = err };
                return null;
            },
            else => {
                self.err = .{ .err = error.UnexpectedDBMessage };
                return null;
            },
        }
    }

    fn read(self: *Listener) !lib.Message {
        var reader = &self._pgreader;
        while (true) {
            const msg = try reader.next();
            switch (msg.type) {
                'N' => {}, // TODO: NoticeResponse
                'E' => return self.setErr(msg.data),
                else => return msg,
            }
        }
    }

    fn setErr(self: *Listener, data: []const u8) error{ PG, OutOfMemory } {
        const allocator = self._allocator;

        // The proto.Error that we're about to create is going to reference data.
        // But data is owned by our Reader and its lifetime doesn't necessarily match
        // what we want here. So we're going to dupe it and make the connection own
        // the data so it can tie its lifecycle to the error.

        // That means clearing out any previous duped error data we had
        if (self._err_data) |err_data| {
            allocator.free(err_data);
        }

        const owned = try allocator.dupe(u8, data);
        self._err_data = owned;
        self.err = .{ .pg = proto.Error.parse(owned) };
        return error.PG;
    }
};

const t = lib.testing;
test "Listener" {
    var rb: [1024]u8 = undefined;
    var wb: [1024]u8 = undefined;

    var stream: lib.PlainStream = try .connect(t.io, .{ .host = "127.0.0.1" });
    defer stream.close();
    var sr = stream.reader(&rb);
    var sw = stream.writer(&wb);
    var l = try Listener.open(t.io, t.allocator, &sr.interface, &sw.interface, .{});
    defer l.deinit();
    try l.auth(t.authOpts(.{}));
    try testListener(&l);
}

// test "Listener: from Pool" {
//     var pool = try lib.Pool.init(t.io, t.allocator, .{
//         .size = 1,
//         .auth = t.authOpts(.{}),
//     });
//     defer pool.deinit();

//     var l = try pool.newListener();
//     defer l.deinit();

//     try testListener(&l);
// }

fn testListener(l: *Listener) !void {
    const io = t.io;
    var reset: std.Io.Event = .unset;
    var tt = try std.Thread.spawn(.{}, struct {
        fn shutdown(io_p: Io, ll: *Listener, r: *std.Io.Event) !void {
            try r.wait(io_p);
            try ll.stop();
        }
    }.shutdown, .{ io, l, &reset });
    tt.detach();

    try l.listen("chan-1", .{});
    try l.listen("chan_2", .{});

    const thrd = try std.Thread.spawn(.{}, testNotifier, .{});
    {
        const notification = l.next().?;
        try t.expectString("chan-1", notification.channel);
        try t.expectString("pl-1", notification.payload);
    }

    {
        const notification = l.next().?;
        try t.expectString("chan_2", notification.channel);
        try t.expectString("pl-2", notification.payload);
    }

    {
        const notification = l.next().?;
        try t.expectString("chan-1", notification.channel);
        try t.expectString("", notification.payload);
    }

    reset.set(io);
    try t.expectEqual(null, l.next());
    thrd.join();
}

fn testNotifier() !void {
    var rb: [1024]u8 = undefined;
    var wb: [1024]u8 = undefined;

    var stream: lib.PlainStream = try .connect(t.io, .{});
    defer stream.close();
    var sr = stream.reader(&rb);
    var sw = stream.writer(&wb);

    var c = try t.connect(&sr.interface, &sw.interface, .{});
    defer c.deinit();
    _ = c.exec("select pg_notify($1, $2)", .{ "chan_x", "pl-x" }) catch unreachable;
    _ = c.exec("select pg_notify($1, $2)", .{ "chan-1", "pl-1" }) catch unreachable;
    _ = c.exec("select pg_notify($1, $2)", .{ "chan_2", "pl-2" }) catch unreachable;
    _ = c.exec("select pg_notify($1, null)", .{"chan-1"}) catch unreachable;
}
