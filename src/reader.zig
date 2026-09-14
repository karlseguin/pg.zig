const std = @import("std");
const lib = @import("lib.zig");

const Allocator = std.mem.Allocator;

// to everyone else, this is our reader
pub const Reader = ReaderT(lib.Stream);

pub const Message = struct {
    type: u8,
    data: []const u8,
};

// Frames PostgreSQL messages out of a std.Io.Reader. Every message is a type
// byte followed by a big-endian u32 length that counts itself but not the type
// byte. Messages that fit the source's buffer are returned as slices into it;
// larger ones are read into a spill allocation. Either way, `Message.data` is
// only valid until the next call.
//
// T is lib.Stream in production and an in-memory stand-in in tests. It must
// provide `reader() *std.Io.Reader`, a `ReadError` set, and
// `getReadError() ReadError`, which recovers the concrete error behind
// error.ReadFailed.
fn ReaderT(comptime T: type) type {
    return struct {
        const Source = switch (@typeInfo(T)) {
            .pointer => |p| p.child,
            else => T,
        };
        pub const Error = Source.ReadError || error{ Closed, InvalidMessageLength, OutOfMemory };

        stream: T,

        // The connection's allocator. Used for spills outside of a flow.
        default_allocator: Allocator,

        // Current allocator; startFlow can swap in a query-specific one.
        allocator: Allocator,

        // Holds a message too large for the source's buffer. Reused for the rest
        // of the flow and freed at endFlow (or deinit) by spill_allocator, the
        // allocator that created it.
        spill: []u8 = &.{},
        spill_allocator: Allocator,

        const Self = @This();

        pub fn init(allocator: Allocator, stream: T) Self {
            return .{
                .stream = stream,
                .allocator = allocator,
                .default_allocator = allocator,
                .spill_allocator = allocator,
            };
        }

        pub fn deinit(self: Self) void {
            self.spill_allocator.free(self.spill);
        }

        // Between startFlow and endFlow the spill buffer is kept and reused: if
        // one row of a result needs it, the following rows probably do too.
        pub fn startFlow(self: *Self, allocator: ?Allocator, timeout_ms: ?u32) !void {
            // TODO: per-query timeouts have not been implemented since the move
            // to std.Io
            _ = timeout_ms;
            self.allocator = allocator orelse self.default_allocator;
        }

        pub fn endFlow(self: *Self) !void {
            self.freeSpill();
            self.allocator = self.default_allocator;
        }

        pub fn next(self: *Self) Error!Message {
            const r = self.stream.reader();
            const header = r.peekArray(5) catch |err| return self.mapError(err);
            const len = try messageLength(header);

            const bytes = if (len <= r.buffer.len)
                r.take(len) catch |err| return self.mapError(err)
            else blk: {
                const dest = try self.spillBuffer(len);
                r.readSliceAll(dest) catch |err| return self.mapError(err);
                break :blk dest;
            };
            return .{ .type = bytes[0], .data = bytes[5..] };
        }

        // Some errors (e.g. an unknown table) are reported by PostgreSQL right
        // after Parse, others (e.g. a duplicate key) only once the result is
        // read. To surface both from query() rather than from the first
        // result.next(), this waits for the next message's header and, only if
        // it is an ErrorResponse, consumes and returns it. Anything else is left
        // for next().
        pub fn peekForError(self: *Self) Error!?[]const u8 {
            const r = self.stream.reader();
            const header = r.peekArray(5) catch |err| return self.mapError(err);
            if (header[0] != 'E') {
                return null;
            }
            const msg = try self.next();
            return msg.data;
        }

        fn spillBuffer(self: *Self, len: usize) Allocator.Error![]u8 {
            const allocator = self.allocator;
            const owner = self.spill_allocator;
            if (self.spill.len < len or allocator.ptr != owner.ptr or allocator.vtable != owner.vtable) {
                self.freeSpill();
                self.spill = try allocator.alloc(u8, len);
                self.spill_allocator = allocator;
                lib.metrics.allocReader(len);
            }
            return self.spill[0..len];
        }

        fn freeSpill(self: *Self) void {
            self.spill_allocator.free(self.spill);
            self.spill = &.{};
        }

        fn mapError(self: *Self, err: std.Io.Reader.Error) Error {
            return switch (err) {
                error.EndOfStream => error.Closed,
                error.ReadFailed => self.stream.getReadError(),
            };
        }
    };
}

// total on-the-wire length, including the type byte
fn messageLength(header: *const [5]u8) error{InvalidMessageLength}!usize {
    const len = std.mem.readInt(u32, header[1..5], .big);
    if (len < 4) {
        return error.InvalidMessageLength;
    }
    return std.math.add(usize, len, 1) catch error.InvalidMessageLength;
}

const t = lib.testing;
const R = ReaderT(*t.Stream);

test "Reader: next" {
    {
        var s = t.Stream.init(10);
        defer s.deinit();
        s.add(&[_]u8{ 8, 0, 0, 0, 4 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();
        const msg = try reader.next();
        try t.expectEqual(8, msg.type);
        try t.expectSlice(u8, &[_]u8{}, msg.data);
    }

    {
        var s = t.Stream.init(10);
        defer s.deinit();
        s.add(&[_]u8{ 1, 0, 0, 0, 5, 2 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();
        const msg = try reader.next();
        try t.expectEqual(1, msg.type);
        try t.expectSlice(u8, &[_]u8{2}, msg.data);
    }

    {
        var s = t.Stream.init(10);
        defer s.deinit();
        s.add(&[_]u8{ 1, 0, 0, 0, 9, 1, 2, 3, 4, 19 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();
        const msg = try reader.next();
        try t.expectEqual(1, msg.type);
        try t.expectSlice(u8, &[_]u8{ 1, 2, 3, 4, 19 }, msg.data);
    }

    {
        // partial 2nd message, but closed without all the data
        var s = t.Stream.init(10);
        defer s.deinit();
        s.add(&[_]u8{ 1, 0, 0, 0, 9, 1, 2, 3, 4, 19, 2 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();
        const msg = try reader.next();
        try t.expectEqual(1, msg.type);
        try t.expectSlice(u8, &[_]u8{ 1, 2, 3, 4, 19 }, msg.data);
        try t.expectError(error.Closed, reader.next());
    }

    {
        // 2 full messages, 2nd message has no data
        var s = t.Stream.init(20);
        defer s.deinit();
        s.add(&[_]u8{ 99, 0, 0, 0, 6, 200, 201, 2, 0, 0, 0, 4 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        const msg1 = try reader.next();
        try t.expectEqual(99, msg1.type);
        try t.expectSlice(u8, &[_]u8{ 200, 201 }, msg1.data);

        const msg2 = try reader.next();
        try t.expectEqual(2, msg2.type);
        try t.expectSlice(u8, &[_]u8{}, msg2.data);
    }

    {
        // 2 full messages, 2nd message has data
        var s = t.Stream.init(20);
        defer s.deinit();
        s.add(&[_]u8{ 99, 0, 0, 0, 6, 200, 201, 3, 0, 0, 0, 7, 1, 8, 2 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        const msg1 = try reader.next();
        try t.expectEqual(99, msg1.type);
        try t.expectSlice(u8, &[_]u8{ 200, 201 }, msg1.data);

        const msg2 = try reader.next();
        try t.expectEqual(3, msg2.type);
        try t.expectSlice(u8, &[_]u8{ 1, 8, 2 }, msg2.data);
    }

    {
        // 2 full messages, split across packets
        var s = t.Stream.init(20);
        defer s.deinit();
        s.add(&[_]u8{ 91, 0, 0, 0, 6, 200, 22, 4, 0, 0, 0, 5 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        const msg1 = try reader.next();
        try t.expectEqual(91, msg1.type);
        try t.expectSlice(u8, &[_]u8{ 200, 22 }, msg1.data);

        s.add(&[_]u8{73});
        const msg2 = try reader.next();
        try t.expectEqual(4, msg2.type);
        try t.expectSlice(u8, &[_]u8{73}, msg2.data);
    }

    {
        // not enough room in buffer for header of 2nd message
        var s = t.Stream.init(8);
        defer s.deinit();
        s.add(&[_]u8{ 17, 0, 0, 0, 4, 5 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        const msg1 = try reader.next();
        try t.expectEqual(17, msg1.type);
        try t.expectSlice(u8, &[_]u8{}, msg1.data);

        s.add(&[_]u8{ 0, 0, 0, 6, 10, 12 });
        const msg2 = try reader.next();
        try t.expectEqual(5, msg2.type);
        try t.expectSlice(u8, &[_]u8{ 10, 12 }, msg2.data);
    }

    {
        // not enough room in buffer for header of 2nd message across multiple calls
        var s = t.Stream.init(8);
        defer s.deinit();
        s.add(&[_]u8{ 17, 0, 0, 0, 5, 1, 200 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        const msg1 = try reader.next();
        try t.expectEqual(17, msg1.type);
        try t.expectSlice(u8, &[_]u8{1}, msg1.data);

        s.add(&[_]u8{ 0, 0 });
        s.add(&[_]u8{0});
        s.add(&[_]u8{ 7, 10, 12, 14 });
        const msg2 = try reader.next();
        try t.expectEqual(200, msg2.type);
        try t.expectSlice(u8, &[_]u8{ 10, 12, 14 }, msg2.data);
    }
}

test "Reader: invalid message length" {
    var s = t.Stream.init(10);
    defer s.deinit();
    s.add(&[_]u8{ 1, 0, 0, 0, 3 });
    var reader = R.init(t.allocator, s);
    defer reader.deinit();
    try t.expectError(error.InvalidMessageLength, reader.next());
}

// simulates message fragmentations
test "Reader: fuzz" {
    var r = t.getRandom();
    const random = r.random();

    const messages = [_]u8{ 1, 0, 0, 0, 4, 2, 0, 0, 0, 5, 1, 3, 0, 0, 0, 6, 1, 2, 4, 0, 0, 0, 24, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 5, 0, 0, 0, 8, 1, 2, 3, 4, 6, 0, 0, 0, 9, 1, 2, 3, 4, 5, 7, 0, 0, 0, 10, 1, 2, 3, 4, 5, 6, 8, 0, 0, 0, 11, 1, 2, 3, 4, 5, 6, 7, 9, 0, 0, 0, 25, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 };

    for (0..400) |_| {
        var s = t.Stream.init(12);
        defer s.deinit();
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        for (0..4) |_| {
            var buf: []const u8 = messages[0..];
            while (buf.len > 0) {
                const l = random.uintAtMost(usize, buf.len - 1) + 1;
                s.add(buf[0..l]);
                buf = buf[l..];
            }

            var arena = std.heap.ArenaAllocator.init(t.allocator);
            defer arena.deinit();

            const allocator: ?Allocator = if (random.uintAtMost(usize, 1) == 1) arena.allocator() else null;

            try reader.startFlow(allocator, null);
            defer reader.endFlow() catch unreachable;

            {
                const msg = try reader.next();
                try t.expectEqual(1, msg.type);
                try t.expectSlice(u8, &[_]u8{}, msg.data);
            }

            {
                const msg = try reader.next();
                try t.expectEqual(2, msg.type);
                try t.expectSlice(u8, &[_]u8{1}, msg.data);
            }

            {
                const msg = try reader.next();
                try t.expectEqual(3, msg.type);
                try t.expectSlice(u8, &[_]u8{ 1, 2 }, msg.data);
            }

            {
                const msg = try reader.next();
                try t.expectEqual(4, msg.type);
                try t.expectSlice(u8, &[_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }, msg.data);
            }

            {
                const msg = try reader.next();
                try t.expectEqual(5, msg.type);
                try t.expectSlice(u8, &[_]u8{ 1, 2, 3, 4 }, msg.data);
            }

            {
                const msg = try reader.next();
                try t.expectEqual(6, msg.type);
                try t.expectSlice(u8, &[_]u8{ 1, 2, 3, 4, 5 }, msg.data);
            }

            {
                const msg = try reader.next();
                try t.expectEqual(7, msg.type);
                try t.expectSlice(u8, &[_]u8{ 1, 2, 3, 4, 5, 6 }, msg.data);
            }

            {
                const msg = try reader.next();
                try t.expectEqual(8, msg.type);
                try t.expectSlice(u8, &[_]u8{ 1, 2, 3, 4, 5, 6, 7 }, msg.data);
            }

            {
                const msg = try reader.next();
                try t.expectEqual(9, msg.type);
                try t.expectSlice(u8, &[_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 }, msg.data);
            }
            try t.expectError(error.Closed, reader.next());
        }
    }
}

test "Reader: spill" {
    {
        //  message bigger than buffer
        var s = t.Stream.init(10);
        defer s.deinit();
        s.add(&[_]u8{ 200, 0, 0, 0, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();
        const msg = try reader.next();
        try t.expectEqual(200, msg.type);
        try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, msg.data);
    }

    {
        //  2nd message bigger than buffer
        var s = t.Stream.init(10);
        defer s.deinit();
        s.add(&[_]u8{ 199, 0, 0, 0, 6, 9, 8, 200, 0, 0, 0, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        const msg1 = try reader.next();
        try t.expectEqual(199, msg1.type);
        try t.expectSlice(u8, &.{ 9, 8 }, msg1.data);

        const msg2 = try reader.next();
        try t.expectEqual(200, msg2.type);
        try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, msg2.data);
    }

    {
        // middle message bigger than buffer
        var s = t.Stream.init(10);
        defer s.deinit();
        s.add(&[_]u8{ 199, 0, 0, 0, 6, 9, 8, 200, 0, 0, 0, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 198, 0, 0, 0, 5, 1 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        const msg1 = try reader.next();
        try t.expectEqual(199, msg1.type);
        try t.expectSlice(u8, &.{ 9, 8 }, msg1.data);

        const msg2 = try reader.next();
        try t.expectEqual(200, msg2.type);
        try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, msg2.data);

        const msg3 = try reader.next();
        try t.expectEqual(198, msg3.type);
        try t.expectSlice(u8, &.{1}, msg3.data);
    }
}

test "Reader: peekForError" {
    {
        // buffered, not an error: left in place for next()
        var s = t.Stream.init(20);
        defer s.deinit();
        s.add(&[_]u8{ 'T', 0, 0, 0, 6, 1, 2, 'E', 0, 0, 0, 5, 9 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        try t.expectEqual(null, try reader.peekForError());
        const msg1 = try reader.next();
        try t.expectEqual('T', msg1.type);
        try t.expectSlice(u8, &.{ 1, 2 }, msg1.data);

        // buffered error: consumed
        const err = (try reader.peekForError()).?;
        try t.expectSlice(u8, &.{9}, err);
        try t.expectError(error.Closed, reader.next());
    }

    {
        // bigger than the buffer, not an error: only the header is read
        var s = t.Stream.init(8);
        defer s.deinit();
        s.add(&[_]u8{ 'D', 0, 0, 0, 12, 1, 2, 3, 4, 5, 6, 7, 8 });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        try t.expectEqual(null, try reader.peekForError());
        const msg = try reader.next();
        try t.expectEqual('D', msg.type);
        try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, msg.data);
    }

    {
        // bigger than the buffer, error: spilled and returned
        var s = t.Stream.init(8);
        defer s.deinit();
        s.add(&[_]u8{ 'E', 0, 0, 0, 12, 1, 2, 3, 4, 5, 6, 7, 8, 'Z', 0, 0, 0, 5, 'I' });
        var reader = R.init(t.allocator, s);
        defer reader.deinit();

        const err = (try reader.peekForError()).?;
        try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, err);
        const msg = try reader.next();
        try t.expectEqual('Z', msg.type);
        try t.expectSlice(u8, &.{'I'}, msg.data);
    }
}

test "Reader: start/endFlow reuses the spill" {
    var s = t.Stream.init(5);
    defer s.deinit();

    // 1st message is bigger than the buffer
    s.add(&[_]u8{ 1, 0, 0, 0, 8, 1, 2, 3, 4 });

    // 2nd message is bigger than first
    s.add(&[_]u8{ 2, 0, 0, 0, 10, 1, 2, 3, 4, 5, 6 });

    // 3rd message is smaller than 2nd (should re-use previous spill)
    s.add(&[_]u8{ 3, 0, 0, 0, 9, 1, 2, 3, 4, 5 });

    var reader = R.init(t.allocator, s);
    defer reader.deinit();

    try reader.startFlow(null, null);
    const msg1 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4 }, msg1.data);
    try t.expectEqual(9, reader.spill.len);

    const msg2 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6 }, msg2.data);
    try t.expectEqual(11, reader.spill.len);

    const msg3 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5 }, msg3.data);
    try t.expectEqual(11, reader.spill.len);
    try reader.endFlow();
    try t.expectEqual(0, reader.spill.len);
}

test "Reader: start/endFlow then a small message" {
    var s = t.Stream.init(7);
    defer s.deinit();

    s.add(&[_]u8{ 1, 0, 0, 0, 8, 1, 2, 3, 4 });
    s.add(&[_]u8{ 2, 0, 0, 0, 10, 1, 2, 3, 4, 5, 6 });
    s.add(&[_]u8{ 3, 0, 0, 0, 9, 1, 2, 3, 4, 5 });

    // 4th message fits in the buffer
    s.add(&[_]u8{ 3, 0, 0, 0, 5, 255 });

    var reader = R.init(t.allocator, s);
    defer reader.deinit();

    try reader.startFlow(null, null);
    const msg1 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4 }, msg1.data);

    const msg2 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6 }, msg2.data);

    const msg3 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5 }, msg3.data);
    try reader.endFlow();

    const msg4 = try reader.next();
    try t.expectSlice(u8, &.{255}, msg4.data);
}

test "Reader: start/endFlow then a large message" {
    var s = t.Stream.init(7);
    defer s.deinit();

    s.add(&[_]u8{ 1, 0, 0, 0, 8, 1, 2, 3, 4 });
    s.add(&[_]u8{ 2, 0, 0, 0, 18, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 });
    s.add(&[_]u8{ 3, 0, 0, 0, 9, 1, 2, 3, 4, 5 });

    // 4rd message is huge
    s.add(&[_]u8{ 4, 0, 0, 19, 140 } ++ "z" ** 5000);

    // 5th message is read outside of the flow and does not fit the buffer
    s.add(&[_]u8{ 5, 0, 0, 0, 11, 255, 250, 245, 240, 235, 230, 225 });

    var reader = R.init(t.allocator, s);
    defer reader.deinit();

    try reader.startFlow(null, null);
    const msg1 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4 }, msg1.data);

    const msg2 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 }, msg2.data);

    const msg3 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5 }, msg3.data);

    const msg4 = try reader.next();
    try t.expectSlice(u8, "z" ** 5000, msg4.data);
    try reader.endFlow();

    const msg5 = try reader.next();
    try t.expectSlice(u8, &.{ 255, 250, 245, 240, 235, 230, 225 }, msg5.data);
}

test "Reader: start/endFlow with flow-specific allocator" {
    defer t.reset();
    var s = t.Stream.init(7);
    defer s.deinit();

    s.add(&[_]u8{ 1, 0, 0, 0, 8, 1, 2, 3, 4 });
    s.add(&[_]u8{ 2, 0, 0, 0, 10, 1, 2, 3, 4, 5, 6 });
    s.add(&[_]u8{ 3, 0, 0, 0, 9, 1, 2, 3, 4, 5 });

    // 4th message is read outside of the flow and does not fit the buffer
    s.add(&[_]u8{ 3, 0, 0, 0, 11, 255, 250, 245, 240, 235, 230, 225 });

    var reader = R.init(t.allocator, s);
    defer reader.deinit();

    try reader.startFlow(t.arena.allocator(), null);
    const msg1 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4 }, msg1.data);

    const msg2 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6 }, msg2.data);

    const msg3 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5 }, msg3.data);
    try reader.endFlow();

    const msg4 = try reader.next();
    try t.expectSlice(u8, &.{ 255, 250, 245, 240, 235, 230, 225 }, msg4.data);
}

test "Reader: spill outlives a flow switch" {
    // a spill from outside a flow must not be reused (or freed) by a flow that
    // uses a different allocator
    defer t.reset();
    var s = t.Stream.init(7);
    defer s.deinit();

    s.add(&[_]u8{ 1, 0, 0, 0, 8, 1, 2, 3, 4 });
    s.add(&[_]u8{ 2, 0, 0, 0, 8, 5, 6, 7, 8 });

    var reader = R.init(t.allocator, s);
    defer reader.deinit();

    const msg1 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4 }, msg1.data);

    try reader.startFlow(t.arena.allocator(), null);
    const msg2 = try reader.next();
    try t.expectSlice(u8, &.{ 5, 6, 7, 8 }, msg2.data);
    try reader.endFlow();
}

test "Reader: startFlow with a spill into deinit" {
    // This can happen on an error case, where we start a flow, but an error
    // happens during processing, causing conn.deinit() to be called (say, when
    // it's released back into the pool in an error state).
    defer t.reset();
    var s = t.Stream.init(7);
    defer s.deinit();

    s.add(&[_]u8{ 1, 0, 0, 0, 8, 1, 2, 3, 4 });

    var reader = R.init(t.allocator, s);
    defer reader.deinit();

    try reader.startFlow(t.arena.allocator(), null);
    const msg1 = try reader.next();
    try t.expectSlice(u8, &.{ 1, 2, 3, 4 }, msg1.data);
}
