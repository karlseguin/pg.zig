const std = @import("std");
const lib = @import("lib.zig");

const log = lib.log;
const Conn = lib.Conn;
const NotificationResponse = lib.proto.NotificationResponse;

const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

pub const Listener = struct {
	conn: Conn,
	err: ?anyerror,

	pub fn open(allocator: Allocator, opts: Conn.ConnectOpts) !Listener {
		return .{
			.err = null,
			.conn = try Conn.open(allocator, opts),
		};
	}

	pub fn deinit(self: *Listener) void {
		self.conn.deinit();
	}

	pub fn auth(self: *Listener, opts: Conn.AuthOpts) !void {
		return self.conn.auth(opts);
	}

	pub fn listen(self: *Listener, channel: []const u8) !void {
		// LISTEN doesn't support parameterized queries. It has to be a simple query.
		// We don't use proto.Query because we want to quote the identifier.

		const buf = &self.conn._buf;
		buf.reset();

		// "LISTEN " = 7
		// "IDENTIFIER" = 128
		// max identifier size is 63, but if we need to quote every character, that's
		// 126. + 2 for the opening and closing quote
		// + 1 for null terminator
		try buf.ensureTotalCapacity(136);
		buf.writeByteAssumeCapacity('Q');

		const len_pos = try buf.skip(4);

		buf.writeAssumeCapacity("LISTEN \"");

		// + 4 for the length itself
		// + 7 for the LISTEN
		// + 2 for the quotes
		// + 1 for the null terminator
		var len = 11 + channel.len + 3;
		for (channel) |c| {
			if (c == '"') {
				len += 1;
				buf.writeAssumeCapacity("\"\"");
			} else {
				buf.writeByteAssumeCapacity(c);
			}
		}
		buf.writeByteAssumeCapacity('"');
		buf.writeByteAssumeCapacity(0);

		// fill in the length
		var view = buf.view(len_pos);
		view.writeIntBig(u32, @intCast(len));

		try self.conn._stream.writeAll(buf.string());

		// we expect a command complete ('C') followed by a ReadyForQuery ('Z')
		const msg = try self.conn.read();
		switch (msg.type) {
			'C' => return self.conn.readyForQuery(),
			else => return error.UnexpectedDBMessage,
		}
	}

	pub fn next(self: *Listener) ?NotificationResponse {
		const msg = self.conn.read() catch |err| {
			self.err = err;
			return null;
		};

		switch (msg.type) {
			'A' => return NotificationResponse.parse(msg.data) catch |err| {
				self.err = err;
				return null;
			},
			else => {
				self.err = error.UnexpectedDBMessage;
				return null;
			}
		}
	}
};

const t = lib.testing;
test "Listener" {
	var l = try Listener.open(t.allocator, .{.host = "localhost"});
	defer l.deinit();
	try l.auth(t.authOpts(.{}));
	try testListener(&l);
}

test "Listener: from Pool" {
	var pool = try lib.Pool.init(t.allocator, .{
		.size = 1,
		.auth = t.authOpts(.{}),
	});
	defer pool.deinit();

	var l = try pool.newListener();
	defer l.deinit();

	try testListener(&l);
}

fn testListener(l: *Listener) !void {
	try l.listen("chan-1");
	try l.listen("chan_2");

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

	thrd.join();
}

fn testNotifier() void {
	var c = t.connect(.{});
	defer c.deinit();
	_ = c.exec("select pg_notify($1, $2)", .{"chan_x", "pl-x"}) catch unreachable;
	_ = c.exec("select pg_notify($1, $2)", .{"chan-1", "pl-1"}) catch unreachable;
	_ = c.exec("select pg_notify($1, $2)", .{"chan_2", "pl-2"}) catch unreachable;
	_ = c.exec("select pg_notify($1, null)", .{"chan-1"}) catch unreachable;
}
