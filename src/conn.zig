const std = @import("std");
const lib = @import("lib.zig");
const SASL = @import("sasl.zig").SASL;
const Buffer = @import("buffer").Buffer;

const proto = lib.proto;
const types = lib.types;
const Reader = lib.Reader;
const Result = lib.Result;
const Timeout = lib.Timeout;

const os = std.os;
const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

pub const Conn = struct {
	stream: Stream,

	// If we get a postgreSQL error, this will be set.
	err: ?proto.Error,

	// The underlying data for err
	_err_data: ?[]const u8,

	// The current transation state, this is whatever the last ReadyForQuery
	// message told us
	_state: u8,

	// A buffer used for writing to PG. This can grow dynamically as needed.
	_buf: Buffer,

	// Used to read data from PG. Has its own buffer which can grow dynamically
	_reader: Reader,

	_allocator: Allocator,

	// Holds information describing the query that we're executing. If the query
	// returns more columns than an appropriately sized ResultState is created as
	// needed.
	_result_state: Result.State,

	// Holds informaiton describing the parameters that PG is expecting. If the
	// query has more parameters, than an appropriately sized one is created.
	// This is separate from _result_state because:
	//   (a) they are populated separately
	//   (b) have distinct lifetimes
	//   (c) they likely have different lengths;
	_param_oids: []i32,

	pub const ConnectOpts = struct {
		host: ?[]const u8 = null,
		port: ?u16 = null,
		write_buffer: ?u16 = null,
		read_buffer: ?u16 = null,
		state_size: u16 = 32,
	};

	pub const StartupOpts = struct {
		username: []const u8 = "postgres",
		password: ?[]const u8 = null,
		database: ?[]const u8 = null,
		timeout: u32 = 10_000,
	};

	pub const QueryOpts = struct {
		timeout: ?u32 = 30_000,
		column_names: bool = false,
		allocator: ?Allocator = null,
	};

	pub fn open(allocator: Allocator, opts: ConnectOpts) !Conn {
		const host = opts.host orelse "127.0.0.1";
		const port = opts.port orelse 5432;

		const address = try std.net.Address.parseIp(host, port);
		const stream = try std.net.tcpConnectToAddress(address);
		errdefer stream.close();

		const buf = try Buffer.init(allocator, opts.write_buffer orelse 2048);
		errdefer buf.deinit();

		const reader = try Reader.init(allocator, opts.read_buffer orelse 4096, stream);
		errdefer reader.deinit();

		const result_state = try Result.State.init(allocator, opts.state_size);
		errdefer result_state.deinit(allocator);

		const param_oids = try allocator.alloc(i32, opts.state_size);
		errdefer param_oids.deinit(allocator);

		return .{
			.err = null,
			.stream = stream,
			._buf = buf,
			._reader = reader,
			._err_data = null,
			._state = 'I', // idle
			._allocator = allocator,
			._param_oids = param_oids,
			._result_state = result_state,
		};
	}

	pub fn deinit(self: Conn) void {
		const allocator = self._allocator;
		if (self._err_data) |err_data| {
			allocator.free(err_data);
		}
		self._buf.deinit();
		self._reader.deinit();
		allocator.free(self._param_oids);
		self._result_state.deinit(allocator);

		// try to send a Terminate to the DB
		self.stream.writeAll(&.{'X', 0, 0, 0, 4}) catch {};
		self.stream.close();
	}

	pub fn startup(self: *Conn, opts: StartupOpts) !void {
		var buf = &self._buf;

		try self._reader.startFlow(opts.timeout);
		defer self._reader.endFlow();

		{
			// write our startup message
			const startup_message = proto.StartupMessage{
				.username = opts.username,
				.database = opts.database orelse opts.username,
			};

			buf.resetRetainingCapacity();
			try startup_message.write(buf);
			try self.stream.writeAll(buf.string());
		}

		var expect_response = true;
		{
			// read the server's response
			const res = try self.read();
			if (res.type != 'R') {
				return self.unexpectedMessage();
			}

			switch (try proto.AuthenticationRequest.parse(res.data)) {
				.ok => expect_response = false,
				.sasl => |sasl| try self.authSASL(opts, sasl),
				.md5 => |salt| try self.authMD5Password(opts, salt),
				.password => try self.authPassword(opts.password orelse ""),
			}
		}

		if (expect_response) {
			// if we're here, it's because we sent more data to the server (e.g. a password)
			// and we're now waiting for a reply, server should send a final auth ok message
			const res = try self.read();
			if (res.type != 'R') {
				return self.unexpectedMessage();
			}
			if (std.meta.activeTag(try proto.AuthenticationRequest.parse(res.data)) != .ok) {
				return self.unexpectedMessage();
			}
		}

		while (true) {
			const msg = try self.read();
			switch (msg.type) {
				'Z' => return,
				'K' => {}, // TODO: BackendKeyData
				'S' => {}, // TODO: ParameterStatus,
				else => return self.unexpectedMessage(),
			}
		}
	}

	pub fn query(self: *Conn, sql: []const u8, values: anytype) !Result {
		return self.queryOpts(sql, values, .{});
	}

	pub fn queryOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !Result {
		var dyn_state = false;
		var number_of_columns: u16 = 0;
		var state = self._result_state;
		const allocator = opts.allocator orelse self._allocator;

		// if we end up creating a dynamic state, we need to clean it up
		errdefer if (dyn_state) state.deinit(allocator);

		var buf = &self._buf;
		buf.reset();

		var param_oids = self._param_oids;
		if (values.len > param_oids.len) {
			param_oids = try allocator.alloc(i32, values.len);
		} else {
			param_oids = param_oids[0..values.len];
		}

		defer {
			if (values.len > self._param_oids.len) {
				// if the above is true, than param_oids was dynamically created just
				// for this query
				allocator.free(param_oids);
			}
		}
		try self._reader.startFlow(opts.timeout);
		// in normal cases, endFlow will be called in result.deinit()
		errdefer self._reader.endFlow();

		// Step 1: Parse, Describe, Sync
		{
			{
				// The first thing we do is send 3 messages: parse, describe, sync.
				// We need the response from describe to put together our Bind message.
				// Specifically, describe will tell us the type of the return columns, and
				// in Bind, we tell the server how we want it to encode each column (text
				// or binary) and to do that, we need to know what they are

				// We can calculate exactly how many bytes our 3 messages are going to be
				// and make sure our buffer is big enough, thus avoiding some unecessary
				// bound checking
				const bind_payload_len = 8 + sql.len;

				// +1 for the type field, 'P'
				// +7 for our Describe message
				// +5 fo rour Sync message
				const total_length = bind_payload_len + 13;

				try buf.ensureTotalCapacity(total_length);
				_ = buf.skip(total_length) catch unreachable;

				// we're sure our buffer is big enough now, views avoid some bound checking
				var view = buf.view(0);

				view.writeByte('P');
				view.writeIntBig(u32, @intCast(bind_payload_len));
				view.writeByte(0); // unnamed stored procedure
				view.write(sql);
				// null terminate sql string, and we'll be specifying 0 parameter types
				view.write(&.{0, 0, 0});

				// DESCRIBE UNNAMED PORTAL + SYNC
				view.write(&.{
					// Describe
					'D',
					// Describe payload length
					0, 0, 0, 6,
					// Describe a prepared statement
					'S',
					// Prepared statement name (unnamed)
					0,
					// Sync
					'S',
					// Sync payload length
					0, 0, 0, 4
				});
				try self.stream.writeAll(buf.string());
			}

			// no longer idle, we're now in a query
			self._state = 'Q';

			// First message we expect back is a ParseComplete, which has no data.
			{
				// If Parse fails, then the server won't reply to our other messages
				// (i.e. Describ) and it'l immediately send a ReadyForQuery.
				const msg = self.read() catch |err| {
					self.readyForQuery() catch {};
					return err;
				};

				if (msg.type != '1') {
					return self.unexpectedMessage();
				}
			}

			{
				// we expect a ParameterDescription message
				const msg = try self.read();
				if (msg.type != 't') {
					return self.unexpectedMessage();
				}

				const data = msg.data;
				if (std.mem.readIntBig(i16, data[0..2]) != param_oids.len) {
					// We weren't given the correct # of parameters. We need to return an
					// error, but the server doesn't know that we're bailing. We still
					// need to read the rest of its messages
					const next = try self.read();
					if (next.type == 'T' or next.type == 'n') {
						self.readyForQuery() catch {};
					} else {
						self.unexpectedMessage() catch {};
					}
					return error.ParameterCount;
				} else {
					var pos: usize = 2;
					for (0..param_oids.len) |i| {
						const end = pos + 4;
						param_oids[i] = std.mem.readIntBig(i32, data[pos..end][0..4]);
						pos = end;
					}
				}
			}

			{
				// We now expect an answer to our describe message.
				// This is either going to be a RowDescription, or a NoData. NoData means
				// our statement doesn't return any data. Either way, we're going to use
				// this information when we generate our Bind message, next.
				const msg = try self.read();
				switch (msg.type) {
					'n' => {}, // no data, number_of_columns = 0
					'T' => {
						const data = msg.data;
						number_of_columns = std.mem.readIntBig(u16, data[0..2]);
						if (number_of_columns > state.oids.len) {
							// we have more columns than our self._result_state can handle, we
							// need to create a new Result.State specifically for this
							dyn_state = true;
							state = try Result.State.init(allocator, number_of_columns);
						}
						const a: ?Allocator = if (opts.column_names) allocator else null;
						try state.from(number_of_columns, data, a);
					},
					else => return self.unexpectedMessage(),
				}
			}

			{
				// finally, we expect a ReadyForQuery response to our Sync
				try self.readyForQuery();
			}
		}

		// Step 2: Bind, Exec, Sync
		{
			buf.resetRetainingCapacity();
			try buf.writeByte('B');

			// length, we'll fill this up once we know
			_ = try buf.skip(4);

			// name of portal and name of prepared statement, we're using an unnamed
			// portal and statement, so these are both empty string, so 2 null terminators
			try buf.write(&.{0, 0});

			// This will take care of parts 1 and 2 - telling PostgreSQL the format that
			// we're sending the values in as well as actually sending the values.
			try types.bindParameters(values, param_oids, buf);

			// We just did a bunch of work binding our values, but would you believe it,
			// we're not done builing our Bind message! The last part is us telling
			// the server what format it should use for each column that it's sending
			// back (if any).
			try types.resultEncoding(state.oids[0..number_of_columns], buf);

			{
				// fill in the length of our Bind message
				var view = buf.view(1);
				// don't include the 'B' type
				view.writeIntBig(u32, @intCast(buf.len() - 1));
			}

			try buf.write(&.{
				'E',
				// message length
				0, 0, 0, 9,
				// unname portal
				0,
				// no row limit
				0, 0, 0, 0,
				// sync
				'S',
				// message length
				0, 0, 0, 4
			});

			try self.stream.writeAll(buf.string());

			{
				const msg = self.read() catch |err| {
					self.readyForQuery() catch {};
					return err;
				};
				if (msg.type != '2') {
					// expecting a BindComplete
					return self.unexpectedMessage();
				}
			}
		}

		self._state = 'Q';

		return .{
			._conn = self,
			._state = state,
			._allocator = allocator,
			._dyn_state = dyn_state,
			._oids = state.oids[0..number_of_columns],
			._values = state.values[0..number_of_columns],
			.column_names = if (opts.column_names) state.names[0..number_of_columns] else &[_][]const u8{},
			.number_of_columns = number_of_columns,
		};
	}

	pub fn scalar(self: *Conn, comptime T: type, sql: []const u8, values: anytype) ScalarReturnType(T) {
		var result = try self.queryOpts(sql, values, .{});
		defer result.deinit();

		const row = (try result.next()) orelse return null;
		const value = row.get(T, 0);
		try result.drain();

		return value;
	}

	// Execute a query that does not return rows
	pub fn exec(self: *Conn, sql: []const u8, values: anytype) !?i64 {
		return self.execOpts(sql, values, .{});
	}

	pub fn execOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !?i64 {
		var buf = &self._buf;
		buf.reset();

		try self._reader.startFlow(opts.timeout);
		defer self._reader.endFlow();

		if (values.len == 0) {
			const simple_query = proto.Query{.sql = sql};
			try simple_query.write(buf);
			// no longer idle, we're now in a query
			self._state = 'Q';
			try self.stream.writeAll(buf.string());
		} else {
			// TODO: there's some optimization opportunities here, since we know
			// we aren't expecting any result. We don't have to ask PG to DESCRIBE
			// the returned columns (there should be none). This is very significant
			// as it would remove 1 back-and-forth. We could just:
			//    Parse + Bind + Exec + Sync
			// Instead of having to do:
			//    Parse + Describe + Sync  ... read response ...  Bind + Exec + Sync
			const result = try self.queryOpts(sql, values, opts);
			result.deinit();
		}

		// affected can be null, so we need a separate boolean to track if we
		// actually have a response.
		var affected: ?i64 = null;
		while (true) {
			const msg = try self.read();
			switch (msg.type) {
				'C' => {
					const cc = try proto.CommandComplete.parse(msg.data);
					affected = cc.rowsAffected();
				},
				'Z' => return affected,
				'T' => affected = 0,
				'D' => affected = (affected orelse 0) + 1,
 				else => return self.unexpectedMessage(),
			}
		}
	}

	fn authSASL(self: *Conn, opts: StartupOpts, req: proto.AuthenticationRequest.SASL) !void {
		if (!req.scram_sha_256) {
			return self.unexpectedMessage();
		}
		var buf = &self._buf;

		var sasl = try SASL.init(self._allocator);
		defer sasl.deinit(self._allocator);

		{
			// send the client initial response
			const msg = proto.SASLInitialResponse{
				.response = sasl.client_first_message,
				.mechanism = "SCRAM-SHA-256",
			};
			buf.resetRetainingCapacity();
			try msg.write(buf);
			try self.stream.writeAll(buf.string());
		}

		{
			// read the server continue response
			const msg = try self.read();
			const c = switch (msg.type) {
				'R' => try proto.AuthenticationSASLContinue.parse(msg.data),
				else => return error.InvalidSASLFlow,
			};
			try sasl.serverResponse(c.data);
		}

		{
			// send the client final response
			const msg = proto.SASLResponse{
				.data = try sasl.clientFinalMessage(opts.password orelse ""),
			};
			buf.resetRetainingCapacity();
			try msg.write(buf);
			try self.stream.writeAll(buf.string());
		}

		{
			// read the server final response
			const msg = try self.read();
			const final = switch (msg.type) {
				'R' => try proto.AuthenticationSASLFinal.parse(msg.data),
				else => return error.InvalidSASLFlow,
			};
			try sasl.verifyServerFinal(final.data);
		}
	}

	fn authMD5Password(self: *Conn, opts: StartupOpts, salt: []const u8) !void {
		var hash: [16]u8 = undefined;
		{
			var hasher = std.crypto.hash.Md5.init(.{});
			hasher.update(opts.password orelse "");
			hasher.update(opts.username);
			hasher.final(&hash);
		}

		{
			var hex_buf:[32]u8 = undefined;
			const hex_hash = try std.fmt.bufPrint(&hex_buf, "{}", .{std.fmt.fmtSliceHexLower(&hash)});
			var hasher = std.crypto.hash.Md5.init(.{});
			hasher.update(hex_hash);
			hasher.update(salt);
			hasher.final(&hash);
		}
		var hashed_password: [35]u8 = undefined;
		const password = try std.fmt.bufPrint(&hashed_password, "md5{}", .{std.fmt.fmtSliceHexLower(&hash)});
		try self.authPassword(password);
	}

	fn authPassword(self: *Conn, password: []const u8) !void {
		const pw = proto.PasswordMessage{.password = password};

		var buf = &self._buf;
		buf.resetRetainingCapacity();
		try pw.write(buf);
		try self.stream.writeAll(buf.string());
	}

	// Should not be called directly
	pub fn read(self: *Conn) !lib.Message {
		var reader = &self._reader;
		while (true) {
			const msg = try reader.next();
			switch (msg.type) {
				'Z' => {
					self._state = msg.data[0];
					return msg;
				},
				'N' => {}, // TODO: NoticeResponse
				'E' => return self.setErr(msg.data),
				else => return msg,
			}
		}
	}

	fn setErr(self: *Conn, data: []const u8) error{PG, OutOfMemory} {
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
		self.err = proto.Error.parse(owned);
		return error.PG;
	}

	fn unexpectedMessage(self: *Conn) error{UnexpectedDBMessage} {
		self._state = 'E';
		return error.UnexpectedDBMessage;
	}

	// should not be called directly
	pub fn readyForQuery(self: *Conn) !void {
		const msg = try self.read();
		if (msg.type != 'Z') {
			return self.unexpectedMessage();
		}
	}
};

fn ScalarReturnType(comptime T: type) type {
	const TT = switch (@typeInfo(T)) {
		.Optional => |opt| opt.child,
		else => T,
	};

	const TTT = switch (TT) {
		[]u8 => []const u8,
		else => TT,
	};

	return anyerror!?TTT;
}

const t = lib.testing;
test "Conn: startup trust (no pass)" {
	var conn = try Conn.open(t.allocator, .{});
	defer conn.deinit();
	try conn.startup(.{.username = "pgz_user_nopass", .database = "postgres"});
}

test "Conn: startup unknown user" {
	var conn = try Conn.open(t.allocator, .{});
	defer conn.deinit();
	try t.expectError(error.PG, conn.startup(.{.username = "does_not_exist"}));
	try t.expectString("password authentication failed for user \"does_not_exist\"", conn.err.?.message);
}

test "Conn: startup cleartext password" {
	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try t.expectError(error.PG, conn.startup(.{.username = "pgz_user_clear"}));
		try t.expectString("empty password returned by client", conn.err.?.message);
	}

	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try t.expectError(error.PG, conn.startup(.{.username = "pgz_user_clear", .password = "wrong"}));
		try t.expectString("password authentication failed for user \"pgz_user_clear\"", conn.err.?.message);
	}

	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try conn.startup(.{.username = "pgz_user_clear", .password = "pgz_user_clear_pw", .database = "postgres"});
	}
}

test "Conn: startup scram-sha-256 password" {
	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try t.expectError(error.PG, conn.startup(.{.username = "pgz_user_scram_sha256"}));
		try t.expectString("password authentication failed for user \"pgz_user_scram_sha256\"", conn.err.?.message);
	}

	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try t.expectError(error.PG, conn.startup(.{.username = "pgz_user_scram_sha256", .password = "wrong"}));
		try t.expectString("password authentication failed for user \"pgz_user_scram_sha256\"", conn.err.?.message);
	}

	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try conn.startup(.{.username = "pgz_user_scram_sha256", .password = "pgz_user_scram_sha256_pw", .database = "postgres"});
	}
}

test "Conn: exec rowsAffected" {
	var c = t.connect(.{});
	defer c.deinit();

	{
		const n = try c.exec("insert into simple_table values ('exec_insert_a'), ('exec_insert_b')", .{});
		try t.expectEqual(2, n.?);
	}

	{
		const n = try c.exec("update simple_table set value = 'exec_insert_a' where value = 'exec_insert_a'", .{});
		try t.expectEqual(1, n.?);
	}

	{
		const n = try c.exec("delete from simple_table where value like 'exec_insert%'", .{});
		try t.expectEqual(2, n.?);
	}

	{
		try t.expectEqual(null, try c.exec("begin", .{}));
		try t.expectEqual(null, try c.exec("end", .{}));
	}
}

test "Conn: exec with values rowsAffected" {
	var c = t.connect(.{});
	defer c.deinit();

	{
		const n = try c.exec("insert into simple_table values ($1), ($2)", .{"exec_insert_args_a", "exec_insert_args_b"});
		try t.expectEqual(2, n.?);
	}
}

test "Conn: exec query that returns rows" {
	var c = t.connect(.{});
	defer c.deinit();
	_ = try c.exec("insert into simple_table values ('exec_sel_1'), ('exec_sel_2')", .{});
	try t.expectEqual(0, c.exec("select * from simple_table where value = 'none'", .{}));
	try t.expectEqual(2, c.exec("select * from simple_table where value like $1", .{"exec_sel_%"}));
}

test "Conn: scalar" {
	var c = t.connect(.{});
	defer c.deinit();

	try t.expectEqual(null, try c.scalar(?i32, "select $1::int", .{null}));
	try t.expectEqual(9876, (try c.scalar(i32, "select $1::int", .{9876})).?);
	try t.expectEqual(null, try c.scalar(?i32, "select $1::int", .{null}));
	try t.expectEqual(1234, (try c.scalar(?i32, "select $1::int", .{1234})).?);
	try t.expectEqual(false, (try c.scalar(bool, "select $1::bool", .{false})).?);
	try t.expectString("abc", (try c.scalar([]u8, "select $1::text", .{"abc"})).?);
	try t.expectEqual(null, try c.scalar(?[]u8, "select $1::text", .{null}));
}

test "Conn: parse error" {
	var c = t.connect(.{});
	defer c.deinit();
	try t.expectError(error.PG, c.query("selct 1", .{}));

	const err = c.err.?;
	try t.expectString("42601", err.code);
	try t.expectString("ERROR", err.severity);
	try t.expectString("syntax error at or near \"selct\"", err.message);

	// connection is still usable
	try t.expectEqual(2, (try c.scalar(i32, "select 2", .{})).?);
}

test "Conn: wrong parameter count" {
	var c = t.connect(.{});
	defer c.deinit();
	try t.expectError(error.ParameterCount, c.query("select $1", .{}));
	try t.expectError(error.ParameterCount, c.query("select $1", .{1, 2}));

	// connection is still usable
	try t.expectEqual(3, (try c.scalar(i32, "select 3", .{})).?);
}

test "Conn: bind error" {
	var c = t.connect(.{});
	defer c.deinit();
	try t.expectError(error.PG, c.query("select $1::bool", .{33.2}));

	// connection is still usable
	try t.expectEqual(4, (try c.scalar(i32, "select 4", .{})).?);
}

test "PG: type support" {
	var c = t.connect(.{});
	defer c.deinit();
	var bytea1 = [_]u8{0, 1};
	var bytea2 = [_]u8{255, 253, 253};

	{
		const result = c.exec(\\
		\\ insert into all_types (
		\\   id,
		\\   col_int2, col_int2_arr,
		\\   col_int4, col_int4_arr,
		\\   col_int8, col_int8_arr,
		\\   col_float4, col_float4_arr,
		\\   col_float8, col_float8_arr,
		\\   col_bool, col_bool_arr,
		\\   col_text, col_text_arr,
		\\   col_bytea, col_bytea_arr,
		\\   col_enum, col_enum_arr,
		\\   col_uuid, col_uuid_arr,
		\\   col_numeric, col_numeric_arr,
		\\   col_timestamp, col_timestamp_arr
		\\ ) values (
		\\   $1,
		\\   $2, $3,
		\\   $4, $5,
		\\   $6, $7,
		\\   $8, $9,
		\\   $10, $11,
		\\   $12, $13,
		\\   $14, $15,
		\\   $16, $17,
		\\   $18, $19,
		\\   $20, $21,
		\\   $22, $23,
		\\   $24, $25
		\\ )
		, .{
			1,
			@as(i16, 382), [_]i16{-9000, 9001},
			@as(i32, -96534), [_]i32{-4929123},
			@as(i64, 8983919283), [_]i64{8888848483,0,-1},
			@as(f32, 1.2345), [_]f32{4.492, -0.000021},
			@as(f64, -48832.3233231), [_]f64{393.291133, 3.1144},
			true, [_]bool{false, true},
			"a text column", [_][]const u8{"it's", "over", "9000"},
			[_]u8{0, 0, 2, 255, 255, 255}, &[_][]u8{&bytea1, &bytea2},
			"val1", [_][]const u8{"val1", "val2"},
			"b7cc282f-ec43-49be-8e09-aafab0104915", [_][]const u8{"166B4751-D702-4FB9-9A2A-CD6B69ED18D6", "ae2f475f-8070-41b7-ba33-86bba8897bde"},
			1234.567, null,
			169804639500713, [_]i64{169804639500713, -94668480000000}
		});
		if (result) |affected| {
			try t.expectEqual(1, affected);
		} else |err| {
			try t.fail(c, err);
		}
	}

	var result = try c.query(
		\\ select
		\\   id,
		\\   col_int2, col_int2_arr,
		\\   col_int4, col_int4_arr,
		\\   col_int8, col_int8_arr,
		\\   col_float4, col_float4_arr,
		\\   col_float8, col_float8_arr,
		\\   col_bool, col_bool_arr,
		\\   col_text, col_text_arr,
		\\   col_bytea, col_bytea_arr,
		\\   col_enum, col_enum_arr,
		\\   col_uuid, col_uuid_arr,
		\\   col_numeric, 'numeric[] placeholder',
		\\   col_timestamp, col_timestamp_arr
		\\ from all_types where id = $1
	, .{1});
	defer result.deinit();

	// used for our arrays
	var arena = std.heap.ArenaAllocator.init(t.allocator);
	defer arena.deinit();

	const aa = arena.allocator();

	const row = (try result.next()) orelse unreachable;
	try t.expectEqual(1, row.get(i32, 0));

	{
		// smallint & smallint[]
		try t.expectEqual(382, row.get(i16, 1));
		try t.expectSlice(i16, &.{-9000, 9001}, try row.iterator(i16, 2).alloc(aa));
	}

	{
		// int & int[]
		try t.expectEqual(-96534, row.get(i32, 3));
		try t.expectSlice(i32, &.{-4929123}, try row.iterator(i32, 4).alloc(aa));
	}

	{
		// bigint & bigint[]
		try t.expectEqual(8983919283, row.get(i64, 5));
		try t.expectSlice(i64, &.{8888848483,0,-1}, try row.iterator(i64, 6).alloc(aa));
	}

	{
		// float4, float4[]
		try t.expectEqual(1.2345, row.get(f32, 7));
		try t.expectSlice(f32, &.{4.492, -0.000021}, try row.iterator(f32, 8).alloc(aa));
	}

	{
		// float8, float8[]
		try t.expectEqual(-48832.3233231, row.get(f64, 9));
		try t.expectSlice(f64, &.{393.291133, 3.1144}, try row.iterator(f64, 10).alloc(aa));
	}

	{
		// bool, bool[]
		try t.expectEqual(true, row.get(bool, 11));
		try t.expectSlice(bool, &.{false, true}, try row.iterator(bool, 12).alloc(aa));
	}

	{
		// text, text[]
		try t.expectString("a text column", row.get([]u8, 13));
		var text_arr = try row.iterator([]const u8, 14).alloc(aa);
		try t.expectEqual(3, text_arr.len);
		try t.expectString("it's", text_arr[0]);
		try t.expectString("over", text_arr[1]);
		try t.expectString("9000", text_arr[2]);
	}

	{
		// bytea, bytea[]
		try t.expectSlice(u8, &.{0, 0, 2, 255, 255, 255}, row.get([]const u8, 15));
		var bytea_arr = try row.iterator([]u8, 16).alloc(aa);
		try t.expectEqual(2, bytea_arr.len);
		try t.expectSlice(u8, &bytea1, bytea_arr[0]);
		try t.expectSlice(u8, &bytea2, bytea_arr[1]);
	}

	{
		// enum, emum[]
		try t.expectString("val1", row.get([]u8, 17));
		var enum_arr = try row.iterator([]const u8, 18).alloc(aa);
		try t.expectEqual(2, enum_arr.len);
		try t.expectString("val1", enum_arr[0]);
		try t.expectString("val2", enum_arr[1]);
	}

	{
		//uuid, uuid[]
		try t.expectSlice(u8, &.{183, 204, 40, 47, 236, 67, 73, 190, 142, 9, 170, 250, 176, 16, 73, 21}, row.get([]u8, 19));
		var uuid_arr = try row.iterator([]const u8, 20).alloc(aa);
		try t.expectEqual(2, uuid_arr.len);
		try t.expectSlice(u8, &.{22, 107, 71, 81, 215, 2, 79, 185, 154, 42, 205, 107, 105, 237, 24, 214}, uuid_arr[0]);
		try t.expectSlice(u8, &.{174, 47, 71, 95, 128, 112, 65, 183, 186, 51, 134, 187, 168, 137, 123, 222}, uuid_arr[1]);
	}

	{
		//decimal
		try t.expectString("1234.567", row.get([]u8, 21));
		try t.expectEqual(1234.567, row.get(f64, 21));
	}

	{
		//timestamp
		try t.expectEqual(169804639500713, row.get(i64, 23));
		try t.expectSlice(i64, &.{169804639500713, -94668480000000}, try row.iterator(i64, 24).alloc(aa));
	}

	try t.expectEqual(null, try result.next());
}

test "PG: null support" {
	var c = t.connect(.{});
	defer c.deinit();
	{
		const result = c.exec(\\
		\\ insert into all_types (
		\\   id,
		\\   col_int2, col_int2_arr,
		\\   col_int4, col_int4_arr,
		\\   col_int8, col_int8_arr,
		\\   col_float4, col_float4_arr,
		\\   col_float8, col_float8_arr,
		\\   col_bool, col_bool_arr,
		\\   col_text, col_text_arr,
		\\   col_bytea, col_bytea_arr,
		\\   col_enum, col_enum_arr,
		\\   col_uuid, col_uuid_arr,
		\\   col_numeric, col_numeric_arr,
		\\   col_timestamp, col_timestamp_arr
		\\ ) values (
		\\   $1,
		\\   $2, $3,
		\\   $4, $5,
		\\   $6, $7,
		\\   $8, $9,
		\\   $10, $11,
		\\   $12, $13,
		\\   $14, $15,
		\\   $16, $17,
		\\   $18, $19,
		\\   $20, $21,
		\\   $22, $23,
		\\   $24, $25
		\\ )
		, .{
			2,
			null, null,
			null, null,
			null, null,
			null, null,
			null, null,
			null, null,
			null, null,
			null, null,
			null, null,
			null, null,
			null, null,
			null, null,
		});
		if (result) |affected| {
			try t.expectEqual(1, affected);
		} else |err| {
			try t.fail(c, err);
		}
	}

	var result = try c.query(
		\\ select
		\\   id,
		\\   col_int2, col_int2_arr,
		\\   col_int4, col_int4_arr,
		\\   col_int8, col_int8_arr,
		\\   col_float4, col_float4_arr,
		\\   col_float8, col_float8_arr,
		\\   col_bool, col_bool_arr,
		\\   col_text, col_text_arr,
		\\   col_bytea, col_bytea_arr,
		\\   col_enum, col_enum_arr,
		\\   col_uuid, col_uuid_arr,
		\\   col_numeric, 'numeric[] placeholder',
		\\   col_timestamp, col_timestamp_arr
		\\ from all_types where id = $1
	, .{2});
	defer result.deinit();

	const row = (try result.next()) orelse unreachable;
	try t.expectEqual(null, row.get(?i16, 1));
	try t.expectEqual(null, row.iterator(?i16, 2));

	try t.expectEqual(null, row.get(?i32, 3));
	try t.expectEqual(null, row.iterator(?i32, 4));

	try t.expectEqual(null, row.get(?i64, 5));
	try t.expectEqual(null, row.iterator(?i64, 6));

	try t.expectEqual(null, row.get(?f32, 7));
	try t.expectEqual(null, row.iterator(?f32, 8));

	try t.expectEqual(null, row.get(?f64, 9));
	try t.expectEqual(null, row.iterator(?f64, 10));

	try t.expectEqual(null, row.get(?bool, 11));
	try t.expectEqual(null, row.iterator(?bool, 12));

	try t.expectEqual(null, row.get(?[]u8, 13));
	try t.expectEqual(null, row.iterator(?[]u8, 14));

	try t.expectEqual(null, row.get(?[]const u8, 15));
	try t.expectEqual(null, row.iterator(?[]const u8, 16));

	try t.expectEqual(null, row.get(?[]const u8, 17));
	try t.expectEqual(null, row.iterator(?[]const u8, 18));

	try t.expectEqual(null, row.get(?[]u8, 19));
	try t.expectEqual(null, row.iterator(?[]const u8, 20));

	try t.expectEqual(null, row.get(?[]u8, 21));
	try t.expectEqual(null, row.get(?f64, 21));

	try t.expectEqual(null, row.get(?i64, 23));
	try t.expectEqual(null, row.iterator(?i64, 24));

	try t.expectEqual(null, try result.next());
}

test "PG: query column names" {
	var c = t.connect(.{});
	defer c.deinit();
	{
		var result = try c.query("select 1 as id, 'leto' as name", .{});
		try t.expectEqual(0, result.column_names.len);
		try result.drain();
		result.deinit();
	}

	{
		var result = try c.queryOpts("select 1 as id, 'leto' as name", .{}, .{.column_names = true});
		defer result.deinit();
		try t.expectEqual(2, result.column_names.len);
		try t.expectString("id", result.column_names[0]);
		try t.expectString("name", result.column_names[1]);
	}
}
