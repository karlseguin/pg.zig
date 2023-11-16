const std = @import("std");
const lib = @import("lib.zig");
const SASL = @import("sasl.zig").SASL;
const Buffer = @import("buffer").Buffer;

const proto = lib.proto;
const types = lib.types;
const Pool = lib.Pool;
const Reader = lib.Reader;
const Result = lib.Result;
const Timeout = lib.Timeout;
const QueryRow = lib.QueryRow;

const os = std.os;
const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

pub const Conn = struct {
	// If we get a postgreSQL error, this will be set.
	err: ?proto.Error,

	_stream: Stream,

	// The underlying data for err
	_err_data: ?[]const u8,

	_pool: ?*Pool = null,

	// The current transation state, this is whatever the last ReadyForQuery
	// message told us
	_state: State,

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

	const State = enum {
		Idle,

		// something bad happened
		Fail,

		// we're doing a query
		Query,

		// we're in a transaction
		Transaction,
	};

	pub const ConnectOpts = struct {
		host: ?[]const u8 = null,
		port: ?u16 = null,
		write_buffer: ?u16 = null,
		read_buffer: ?u16 = null,
		result_state_size: u16 = 32,
	};

	pub const AuthOpts = struct {
		username: []const u8 = "postgres",
		password: ?[]const u8 = null,
		database: ?[]const u8 = null,
		timeout: u32 = 10_000,
	};

	pub const QueryOpts = struct {
		timeout: ?u32 = null,
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

		const result_state = try Result.State.init(allocator, opts.result_state_size);
		errdefer result_state.deinit(allocator);

		const param_oids = try allocator.alloc(i32, opts.result_state_size);
		errdefer param_oids.deinit(allocator);

		return .{
			.err = null,
			._buf = buf,
			._reader = reader,
			._stream = stream,
			._err_data = null,
			._state = .Idle,
			._allocator = allocator,
			._param_oids = param_oids,
			._result_state = result_state,
		};
	}

	pub fn deinit(self: *Conn) void {
		const allocator = self._allocator;
		if (self._err_data) |err_data| {
			allocator.free(err_data);
		}
		self._buf.deinit();
		self._reader.deinit();
		allocator.free(self._param_oids);
		self._result_state.deinit(allocator);

		// try to send a Terminate to the DB
		self.write(&.{'X', 0, 0, 0, 4}) catch {};
		self._stream.close();
	}

	pub fn release(self: *Conn) void {
		var pool = self._pool orelse {
			self.deinit();
			return;
		};
		self.err = null;
		pool.release(self);
	}

	pub fn auth(self: *Conn, opts: AuthOpts) !void {
		var buf = &self._buf;

		try self._reader.startFlow(null, opts.timeout);
		defer self._reader.endFlow() catch {
			// this can only fail in extreme conditions (OOM) and it will only impact
			// the next query (and if the app is using the pool, the pool will try to
			// recover from this anyways)
			self._state = .Fail;
		};

		{
			// write our startup message
			const startup_message = proto.StartupMessage{
				.username = opts.username,
				.database = opts.database orelse opts.username,
			};

			buf.resetRetainingCapacity();
			try startup_message.write(buf);
			try self.write(buf.string());
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
		if (self.canQuery() == false) {
			return error.ConnectionBusy;
		}

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
		try self._reader.startFlow(allocator, opts.timeout);
		// in normal cases, endFlow will be called in result.deinit()
		errdefer self._reader.endFlow() catch {
			// this can only fail in extreme conditions (OOM) and it will only impact
			// the next query (and if the app is using the pool, the pool will try to
			// recover from this anyways)
				self._state = .Fail;
		};

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
				try self.write(buf.string());
			}

			// no longer idle, we're now in a query
			self._state = .Query;

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
				if (std.mem.readInt(i16, data[0..2], .big) != param_oids.len) {
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
						param_oids[i] = std.mem.readInt(i32, data[pos..end][0..4], .big);
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
						number_of_columns = std.mem.readInt(u16, data[0..2], .big);
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

			try self.write(buf.string());

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

		// our call to readyForQuery above changed the state, but as far as we're
		// concerned, we're still doing the query.
		self._state = .Query;

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

	pub fn row(self: *Conn, sql: []const u8, values: anytype) !?QueryRow {
		return self.rowOpts(sql, values, .{});
	}

	pub fn rowOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !?QueryRow {
		if (self.canQuery() == false) {
			return error.ConnectionBusy;
		}

		var result = try self.queryOpts(sql, values, opts);
		errdefer result.deinit();

		const r = try result.next() orelse {
			result.deinit();
			return null;
		};

		if (try result.next() != null) {
			try result.drain();
			result.deinit();
			return error.MoreThanOneRow;
		}

		return .{
			.row = r,
			.result = result,
		};
	}

	// Execute a query that does not return rows
	pub fn exec(self: *Conn, sql: []const u8, values: anytype) !?i64 {
		return self.execOpts(sql, values, .{});
	}

	pub fn execOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !?i64 {
		if (self.canQuery() == false) {
			return error.ConnectionBusy;
		}
		var buf = &self._buf;
		buf.reset();

		try self._reader.startFlow(opts.allocator, opts.timeout);
		defer self._reader.endFlow() catch {
			// this can only fail in extreme conditions (OOM) and it will only impact
			// the next query (and if the app is using the pool, the pool will try to
			// recover from this anyways)
			self._state = .Fail;
		};

		if (values.len == 0) {
			const simple_query = proto.Query{.sql = sql};
			try simple_query.write(buf);
			// no longer idle, we're now in a query
			self._state = .Query;
			try self.write(buf.string());
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
			const msg = self.read() catch |err| {
				if (err == error.PG) {
					self.readyForQuery() catch {};
				}
				return err;
			};
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

	pub fn begin(self: *Conn) !void {
		self._state = .Transaction;
		_ = try self.execOpts("begin", .{}, .{});
	}

	pub fn commit(self: *Conn) !void {
		_ = try self.execOpts("commit", .{}, .{});
	}

	pub fn rollback(self: *Conn) !void {
		_ = try self.execOpts("rollback", .{}, .{});
	}

	fn authSASL(self: *Conn, opts: AuthOpts, req: proto.AuthenticationRequest.SASL) !void {
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
			try self.write(buf.string());
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
			try self.write(buf.string());
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

	fn authMD5Password(self: *Conn, opts: AuthOpts, salt: []const u8) !void {
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
		try self.write(buf.string());
	}

	// Should not be called directly
	pub fn read(self: *Conn) !lib.Message {
		var reader = &self._reader;
		while (true) {
			const msg = reader.next() catch |err| {
				self._state = .Fail;
				return err;
			};
			switch (msg.type) {
				'Z' => {
					self._state = switch (msg.data[0]) {
						'I' => .Idle,
						'T' => .Transaction,
						'E' => .Fail,
						else => unreachable,
					};
					return msg;
				},
				'N' => {}, // TODO: NoticeResponse
				'E' => return self.setErr(msg.data),
				else => return msg,
			}
		}
	}

	fn write(self: *Conn, data: []const u8) !void {
		self._stream.writeAll(data) catch |err| {
			self._state = .Fail;
			return err;
		};
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
		self._state = .Fail;
		return error.UnexpectedDBMessage;
	}

	fn canQuery(self: *const Conn) bool {
		const state = self._state;
		if (state == .Idle or state == .Transaction) {
			return true;
		}
		return false;
	}

	// should not be called directly
	pub fn readyForQuery(self: *Conn) !void {
		const msg = try self.read();
		if (msg.type != 'Z') {
			return self.unexpectedMessage();
		}
	}
};

const t = lib.testing;
test "Conn: auth trust (no pass)" {
	var conn = try Conn.open(t.allocator, .{});
	defer conn.deinit();
	try conn.auth(.{.username = "pgz_user_nopass", .database = "postgres"});
}

test "Conn: auth unknown user" {
	var conn = try Conn.open(t.allocator, .{});
	defer conn.deinit();
	try t.expectError(error.PG, conn.auth(.{.username = "does_not_exist"}));
	try t.expectString("password authentication failed for user \"does_not_exist\"", conn.err.?.message);
}

test "Conn: auth cleartext password" {
	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try t.expectError(error.PG, conn.auth(.{.username = "pgz_user_clear"}));
		try t.expectString("empty password returned by client", conn.err.?.message);
	}

	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try t.expectError(error.PG, conn.auth(.{.username = "pgz_user_clear", .password = "wrong"}));
		try t.expectString("password authentication failed for user \"pgz_user_clear\"", conn.err.?.message);
	}

	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try conn.auth(.{.username = "pgz_user_clear", .password = "pgz_user_clear_pw", .database = "postgres"});
	}
}

test "Conn: auth scram-sha-256 password" {
	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try t.expectError(error.PG, conn.auth(.{.username = "pgz_user_scram_sha256"}));
		try t.expectString("password authentication failed for user \"pgz_user_scram_sha256\"", conn.err.?.message);
	}

	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try t.expectError(error.PG, conn.auth(.{.username = "pgz_user_scram_sha256", .password = "wrong"}));
		try t.expectString("password authentication failed for user \"pgz_user_scram_sha256\"", conn.err.?.message);
	}

	{
		var conn = try Conn.open(t.allocator, .{});
		defer conn.deinit();
		try conn.auth(.{.username = "pgz_user_scram_sha256", .password = "pgz_user_scram_sha256_pw", .database = "postgres"});
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

test "Conn: parse error" {
	var c = t.connect(.{});
	defer c.deinit();
	try t.expectError(error.PG, c.query("selct 1", .{}));

	const err = c.err.?;
	try t.expectString("42601", err.code);
	try t.expectString("ERROR", err.severity);
	try t.expectString("syntax error at or near \"selct\"", err.message);

	// connection is still usable
	try t.expectEqual(2, t.scalar(&c, "select 2"));
}

test "Conn: wrong parameter count" {
	var c = t.connect(.{});
	defer c.deinit();
	try t.expectError(error.ParameterCount, c.query("select $1", .{}));
	try t.expectError(error.ParameterCount, c.query("select $1", .{1, 2}));

	// connection is still usable
	try t.expectEqual(3, t.scalar(&c, "select 3"));
}

test "Conn: bind error" {
	var c = t.connect(.{});
	defer c.deinit();
	try t.expectError(error.PG, c.query("select $1::bool", .{33.2}));

	// connection is still usable
	try t.expectEqual(4, t.scalar(&c, "select 4"));
}

test "Conn: Query within Query error" {
	var c = t.connect(.{});
	defer c.deinit();
	var rows = try c.query("select 1", .{});
	defer rows.deinit();

	try t.expectError(error.ConnectionBusy, c.row("select 2", .{}));
	try t.expectEqual(1, (try rows.next()).?.get(i32 ,0));
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
		\\   col_timestamp, col_timestamp_arr,
		\\   col_json, col_json_arr,
		\\   col_jsonb, col_jsonb_arr,
		\\   col_char, col_char_arr,
		\\   col_charn, col_charn_arr,
		\\   col_timestamptz, col_timestamptz_arr
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
		\\   $24, $25,
		\\   $26, $27,
		\\   $28, $29,
		\\   $30, $31,
		\\   $32, $33,
		\\   $34, $35
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
			1234.567, [_]f64{0, -1.1, std.math.nan(f64), std.math.inf(f32), 12345.000101},
			169804639500713, [_]i64{169804639500713, -94668480000000},
			"{\"count\":1.3}", [_][]const u8{"[1,2,3]", "{\"rows\":[{\"a\": true}]}"},
			"{\"over\":9000}", [_][]const u8{"[true,false]", "{\"cols\":[{\"z\": 0.003}]}"},
			79, [_]u8{'1', 'z', '!'},
			"Teg", [_][]const u8{&.{78, 82}, "hi"},
			169804639500713, [_]i64{169804639500713, -94668480000000},
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
		\\   col_numeric, col_numeric_arr,
		\\   col_timestamp, col_timestamp_arr,
		\\   col_json, col_json_arr,
		\\   col_jsonb, col_jsonb_arr,
		\\   col_char, col_char_arr,
		\\   col_charn, col_charn_arr,
		\\   col_timestamptz, col_timestamptz_arr
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
		const arr = try row.iterator([]const u8, 14).alloc(aa);
		try t.expectEqual(3, arr.len);
		try t.expectString("it's", arr[0]);
		try t.expectString("over", arr[1]);
		try t.expectString("9000", arr[2]);
	}

	{
		// bytea, bytea[]
		try t.expectSlice(u8, &.{0, 0, 2, 255, 255, 255}, row.get([]const u8, 15));
		const arr = try row.iterator([]u8, 16).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectSlice(u8, &bytea1, arr[0]);
		try t.expectSlice(u8, &bytea2, arr[1]);
	}

	{
		// enum, emum[]
		try t.expectString("val1", row.get([]u8, 17));
		const arr = try row.iterator([]const u8, 18).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("val1", arr[0]);
		try t.expectString("val2", arr[1]);
	}

	{
		//uuid, uuid[]
		try t.expectSlice(u8, &.{183, 204, 40, 47, 236, 67, 73, 190, 142, 9, 170, 250, 176, 16, 73, 21}, row.get([]u8, 19));
		const arr = try row.iterator([]const u8, 20).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectSlice(u8, &.{22, 107, 71, 81, 215, 2, 79, 185, 154, 42, 205, 107, 105, 237, 24, 214}, arr[0]);
		try t.expectSlice(u8, &.{174, 47, 71, 95, 128, 112, 65, 183, 186, 51, 134, 187, 168, 137, 123, 222}, arr[1]);
	}

	{
		// numeric, numeric[]
		try t.expectEqual(1234.567, row.get(f64, 21));
		const arr = try row.iterator(lib.Numeric, 22).alloc(aa);
		try t.expectEqual(5, arr.len);
		try expectNumeric(arr[0], "0.0");
		try expectNumeric(arr[1], "-1.1");
		try expectNumeric(arr[2], "nan");
		try expectNumeric(arr[3], "inf");
		try expectNumeric(arr[4], "12345.000101");
	}

	{
		//timestamp
		try t.expectEqual(169804639500713, row.get(i64, 23));
		try t.expectSlice(i64, &.{169804639500713, -94668480000000}, try row.iterator(i64, 24).alloc(aa));
	}

	{
		// json, json[]
		try t.expectString("{\"count\":1.3}", row.get([]u8, 25));
		const arr = try row.iterator([]const u8, 26).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("[1,2,3]", arr[0]);
		try t.expectString("{\"rows\":[{\"a\": true}]}", arr[1]);
	}

	{
		// jsonb, jsonb[]
		try t.expectString("{\"over\": 9000}", row.get([]u8, 27));
		const arr = try row.iterator([]const u8, 28).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("[true, false]", arr[0]);
		try t.expectString("{\"cols\": [{\"z\": 0.003}]}", arr[1]);
	}

	{
		// char, char[]
		try t.expectEqual(79, row.get(u8, 29));
		const arr = try row.iterator(u8, 30).alloc(aa);
		try t.expectEqual(3, arr.len);
		try t.expectEqual('1', arr[0]);
		try t.expectEqual('z', arr[1]);
		try t.expectEqual('!', arr[2]);
	}

	{
		// charn, charn[]
		try t.expectString("Teg", row.get([]u8, 31));
		const arr = try row.iterator([]u8, 32).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("NR", arr[0]);
		try t.expectString("hi", arr[1]);
	}

	{
		//timestamp
		try t.expectEqual(169804639500713, row.get(i64, 33));
		try t.expectSlice(i64, &.{169804639500713, -94668480000000}, try row.iterator(i64, 34).alloc(aa));
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
		\\   col_timestamp, col_timestamp_arr,
		\\   col_json, col_json_arr,
		\\   col_jsonb, col_jsonb_arr,
		\\   col_char, col_char_arr,
		\\   col_charn, col_charn_arr
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
		\\   $24, $25,
		\\   $26, $27,
		\\   $28, $29,
		\\   $30, $31,
		\\   $32, $33
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
		\\   col_timestamp, col_timestamp_arr,
		\\   col_json, col_json_arr,
		\\   col_jsonb, col_jsonb_arr,
		\\   col_char, col_char_arr,
		\\   col_charn, col_charn_arr
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

	try t.expectEqual(null, row.get(?[]u8, 25));
	try t.expectEqual(null, row.iterator(?[]const u8, 26));

	try t.expectEqual(null, row.get(?[]u8, 27));
	try t.expectEqual(null, row.iterator(?[]const u8, 28));

	try t.expectEqual(null, row.get(?u8, 29));
	try t.expectEqual(null, row.iterator(?u8, 30));

	try t.expectEqual(null, row.get(?u8, 31));
	try t.expectEqual(null, row.iterator(?u8, 32));

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

test "PG: JSON struct" {
	var c = t.connect(.{});
	defer c.deinit();

	{
		const result = c.exec(\\
		\\ insert into all_types (id, col_json, col_jsonb)
		\\ values ($1, $2, $3)
		, .{3, DummyStruct{.id = 1, .name = "Leto"}, &DummyStruct{.id = 2, .name = "Ghanima"}});

		if (result) |affected| {
			try t.expectEqual(1, affected);
		} else |err| {
			try t.fail(c, err);
		}
	}

	var result = try c.query("select col_json, col_jsonb from all_types where id = $1", .{3});
	defer result.deinit();

	const row = (try result.next()) orelse unreachable;
	try t.expectString("{\"id\":1,\"name\":\"Leto\"}", row.get([]u8, 0));
	try t.expectString("{\"id\": 2, \"name\": \"Ghanima\"}", row.get(?[]const u8, 1).?);
}

test "PG: row" {
	var c = t.connect(.{});
	defer c.deinit();

	const r1 = try c.row("select 1 where $1", .{false});
	try t.expectEqual(null, r1);

	const r2 = (try c.row("select 2 where $1", .{true})) orelse unreachable;
	try t.expectEqual(2, r2.get(i32, 0));
	r2.deinit();

	// make sure the conn is still valid after a successful row
	const r3 = (try c.row("select $1::int where $2", .{3, true})) orelse unreachable;
	try t.expectEqual(3, r3.get(i32, 0));
	r3.deinit();

	// make sure the conn is still valid after a successful row
	try t.expectError(error.MoreThanOneRow, c.row("select 1 union all select 2", .{}));

	// make sure the conn is still valid after MoreThanOneRow error
	const r4 = (try c.row("select $1::text where $2", .{"hi", true})) orelse unreachable;
	try t.expectString("hi", r4.get([]u8, 0));
	r4.deinit();
}

test "PG: begin/commit" {
	var c = t.connect(.{});
	defer c.deinit();

	try c.begin();
	_ = try c.exec("delete from simple_table", .{});
	_ = try c.exec("insert into simple_table values ($1)", .{"begin_commit"});
	try c.commit();

	const row = (try c.row("select value from simple_table", .{})).?;
	defer row.deinit();

	try t.expectString("begin_commit", row.get([]u8, 0));
}

test "PG: begin/rollback" {
	var c = t.connect(.{});
	defer c.deinit();

	_ = try c.exec("delete from simple_table", .{});
	try c.begin();
	_ = try c.exec("insert into simple_table values ($1)", .{"begin_commit"});
	try c.rollback();

	const row = try c.row("select value from simple_table", .{});
	try t.expectEqual(null, row);
}

test "PG: bind enums" {
	var c = t.connect(.{});
	defer c.deinit();

	_ = try c.exec(
		\\ insert into all_types (id, col_enum, col_enum_arr, col_text, col_text_arr)
		\\ values (4, $1, $2, $3, $4)
	, .{DummyEnum.val1, &[_]DummyEnum{DummyEnum.val1, DummyEnum.val2}, DummyEnum.val2, [_]DummyEnum{DummyEnum.val2, DummyEnum.val1}});

	const row = (try c.row(
		\\ select col_enum, col_text, col_enum_arr, col_text_arr
		\\ from all_types
		\\ where id = 4
	, .{})) orelse unreachable;
	defer row.deinit();

	try t.expectString("val1", row.get([]u8, 0));
	try t.expectString("val2", row.get([]u8, 1));

	var arena = std.heap.ArenaAllocator.init(t.allocator);
	defer arena.deinit();
	const aa = arena.allocator();

	{
		const arr = try row.iterator([]const u8, 2).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("val1", arr[0]);
		try t.expectString("val2", arr[1]);
	}

	{
		const arr = try row.iterator([]const u8, 3).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("val2", arr[0]);
		try t.expectString("val1", arr[1]);
	}
}

test "PG: numeric" {
	var c = t.connect(.{});
	defer c.deinit();
	// _ = try c.exec("insert into all_types (id, col_numeric) values (999, $1)", .{-});

	{
		// read
		const row = (try c.row(
			\\ select 'nan'::numeric, '+Inf'::numeric, '-Inf'::numeric,
			\\ 0::numeric, 0.0::numeric, -0.00009::numeric, -999999.888880::numeric,
			\\ 0.000008, 999999.888807::numeric, 123456.78901234::numeric(14, 8)
		, .{})).?;
		defer row.deinit();

		try t.expectEqual(true, std.math.isNan(row.get(f64, 0)));
		try t.expectEqual(true, std.math.isInf(row.get(f64, 1)));
		try t.expectEqual(true, std.math.isNegativeInf(row.get(f64, 2)));
		try t.expectEqual(0, row.get(f64, 3));
		try t.expectEqual(0, row.get(f64, 4));
		try t.expectEqual(-0.00009, row.get(f64, 5));
		try t.expectEqual(-999999.888880, row.get(f64, 6));
		try t.expectEqual(0.000008, row.get(f64, 7));
		try t.expectEqual(999999.888807, row.get(f64, 8));
		try t.expectEqual(123456.78901234, row.get(f64, 9));
	}

	{
		// write + write
		const row = (try c.row(
			\\ select
			\\   $1::numeric, $2::numeric, $3::numeric,
			\\   $4::numeric, $5::numeric, $6::numeric,
			\\   $7::numeric, $8::numeric, $9::numeric,
			\\   $10::numeric, $11::numeric, $12::numeric,
			\\   $13::numeric, $14::numeric, $15::numeric,
			\\   $16::numeric, $17::numeric, $18::numeric,
			\\   $19::numeric, $20::numeric, $21::numeric,
			\\   $22::numeric, $23::numeric, $24::numeric,
			\\   $25::numeric, $26::numeric
		, .{
			-0.00089891, 939293122.0001101, "-123.4560991",
			std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64),
			std.math.nan(f32), std.math.inf(f32), -std.math.inf(f32),
			1.1, 12.98, 123.987,
			1234.9876, 12345.98765, 123456.987654,
			1234567.9876543, 12345678.98765432, 123456789.987654321,
			@as(f64, 0), @as(f64, 1), 0,
			1, 999999999.9999999, @as(f64, 999999999.9999999),
			-999999999.9999999, @as(f64, -999999999.9999999)
		})).?;
		defer row.deinit();

		try expectNumeric(row.get(lib.Numeric, 0), "-0.00089891");
		try expectNumeric(row.get(lib.Numeric, 1), "939293122.0001101");
		try expectNumeric(row.get(lib.Numeric, 2), "-123.4560991");

		try expectNumeric(row.get(lib.Numeric, 3), "nan");
		try expectNumeric(row.get(lib.Numeric, 4), "inf");
		try expectNumeric(row.get(lib.Numeric, 5), "-inf");

		try expectNumeric(row.get(lib.Numeric, 6), "nan");
		try expectNumeric(row.get(lib.Numeric, 7), "inf");
		try expectNumeric(row.get(lib.Numeric, 8), "-inf");

		try expectNumeric(row.get(lib.Numeric, 9), "1.1");
		try expectNumeric(row.get(lib.Numeric, 10), "12.98");
		try expectNumeric(row.get(lib.Numeric, 11), "123.987");
		try expectNumeric(row.get(lib.Numeric, 12), "1234.9876");
		try expectNumeric(row.get(lib.Numeric, 13), "12345.98765");
		try expectNumeric(row.get(lib.Numeric, 14), "123456.987654");
		try expectNumeric(row.get(lib.Numeric, 15), "1234567.9876543");
		try expectNumeric(row.get(lib.Numeric, 16), "12345678.98765432");
		try expectNumeric(row.get(lib.Numeric, 17), "123456789.98765433");
		try expectNumeric(row.get(lib.Numeric, 18), "0.0");
		try expectNumeric(row.get(lib.Numeric, 19), "1.0");
		try expectNumeric(row.get(lib.Numeric, 20), "0.0");
		try expectNumeric(row.get(lib.Numeric, 21), "1.0");
		try expectNumeric(row.get(lib.Numeric, 22), "999999999.9999999");
		try expectNumeric(row.get(lib.Numeric, 23), "999999999.9999999");
		try expectNumeric(row.get(lib.Numeric, 24), "-999999999.9999999");
		try expectNumeric(row.get(lib.Numeric, 25), "-999999999.9999999");


		const numeric = row.get(lib.Numeric, 1);
		try t.expectEqual(939293122.0001101, numeric.toFloat());
		try t.expectEqual(2, numeric.weight);
		try t.expectEqual(.positive, numeric.sign);
		try t.expectEqual(7, numeric.scale);
		try t.expectSlice(u8, &.{0, 9, 15, 89, 12, 50, 0, 1, 3, 242}, numeric.digits);
	}
}

fn expectNumeric(numeric: lib.Numeric, expected: []const u8) !void {
	var str_buf: [50]u8 = undefined;
	try t.expectString(expected, numeric.toString(&str_buf));

	var a = try t.allocator.alloc(u8, numeric.estimatedStringLen());
	defer t.allocator.free(a);
	try t.expectString(expected, numeric.toString(a));

	if (std.mem.eql(u8, expected, "nan")) {
		try t.expectEqual(true, std.math.isNan(numeric.toFloat()));
	} else if (std.mem.eql(u8, expected, "inf")) {
		try t.expectEqual(true, std.math.isInf(numeric.toFloat()));
	} else if (std.mem.eql(u8, expected, "-inf")) {
		try t.expectEqual(true, std.math.isNegativeInf(numeric.toFloat()));
	} else {
		try t.expectDelta(try std.fmt.parseFloat(f64, expected), numeric.toFloat(), 0.000001);
	}
}

const DummyStruct = struct{
	id: i32,
	name: []const u8,
};

const DummyEnum = enum {
	val1,
	val2,
};
