const std = @import("std");
const lib = @import("lib.zig");
const Buffer = @import("buffer").Buffer;

const proto = lib.proto;
const types = lib.types;
const Reader = lib.Reader;
const Result = lib.Result;
const Timeout = lib.Timeout;
const QueryState = lib.QueryState;

const os = std.os;
const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

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
};

pub const Conn = struct {
	stream: Stream,

	// If we get a postgreSQL error, this will be set.
	err: ?proto.Error,

	// The underlying data for err
	_err_data: ?[]const u8,

	// A buffer used for writing to PG. This can grow dynamically as needed.
	_buf: Buffer,

	// Used to read data from PG. Has its own buffer which can grow dynamically
	_reader: Reader,

	_allocator: Allocator,

	// Holds information describing the query that we're executing. If the query
	// returns more columns than an appropriately sized QueryState is created as
	// needed.
	_query_state: QueryState,

	// Holds informaiton describing the parameters that PG is expecting. If the
	// query has more parameters, than an appropriately sized one is created.
	// This is separate from _query_state because:
	//   (a) they are populated separately
	//   (b) have distinct lifetimes
	//   (c) they likely have different lengths;
	_param_oids: []i32,

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

		const query_state = try QueryState.init(allocator, opts.state_size);
		errdefer query_state.deinit(allocator);

		const param_oids = try allocator.alloc(i32, opts.state_size);
		errdefer param_oids.deinit(allocator);

		return .{
			.err = null,
			.stream = stream,
			._buf = buf,
			._reader = reader,
			._err_data = null,
			._allocator = allocator,
			._param_oids = param_oids,
			._query_state = query_state,
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
		self._query_state.deinit(allocator);

		// try to send a Terminate to the DB
		self.stream.writeAll(&.{'X', 0, 0, 0, 4}) catch {};
		self.stream.close();
	}

	pub fn startup(self: *Conn, opts: StartupOpts) !void {
		var buf = &self._buf;

		const timeout = Timeout.init(opts.timeout, self.stream);
		try timeout.forSendAndRecv();
		errdefer _ = timeout.clear() catch {};

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

		{
			// read the server's response
			const res = try self.read();
			if (res.type != 'R') {
				return error.UnexpectedDBMessage;
			}

			switch (try proto.AuthenticationRequest.parse(res.data)) {
				.ok => return,
				.sasl => |sasl| try self.authSASL(opts, sasl),
				.md5 => |salt| try self.authMD5Password(opts, salt),
				.password => try self.authPassword(opts.password orelse ""),
			}
		}

		{
			// if we're here, it's because we sent more data to the server (e.g. a password)
			// and we're now waiting for a reply, server should send a final auth ok message
			const res = try self.read();
			if (res.type != 'R') {
				return error.UnexpectedDBMessage;
			}
			if (std.meta.activeTag(try proto.AuthenticationRequest.parse(res.data)) != .ok) {
				return error.UnexpectedDBMessage;
			}
		}

		while (true) {
			const msg = try self.read();
			switch (msg.type) {
				'Z' => return,  // ready for query
				'K' => {}, // TODO: BackendKeyData
				'S' => {}, // TODO: ParameterStatus,
				else => return error.UnexpectedDBMessage,
			}
			if (timeout.expired()) {
				return error.Timeout;
			}
		}
	}

	pub fn query(self: *Conn, sql: []const u8, values: anytype) !Result {
		return self.queryOpts(sql, values, .{});
	}

	pub fn queryOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !Result {
		_ = opts;

		var dyn_state = false;
		var number_of_columns: u16 = 0;
		var state = self._query_state;
		const allocator = self._allocator;

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

			// First message we expect back is a ParseComplete, which has no data.
			parse_complete: while (true) {
				const msg = try self.read();
				switch (msg.type) {
					'Z' => {}, // ready for query, from some previous state
					'1' => break :parse_complete, // 1 ==> ParseComplete
					else => return error.UnexpectedDBMessage,
				}
			}

			{
				// we expect a ParameterDescription message, which we won't use
				const msg = try self.read();
				if (msg.type != 't') {
					return error.UnexpectedDBMessage;
				}

				const data = msg.data;
				std.debug.assert(values.len == std.mem.readIntBig(i16, data[0..2]));
				var pos: usize = 2;
				for (0..param_oids.len) |i| {
					const end = pos + 4;
					param_oids[i] = std.mem.readIntBig(i32, data[pos..end][0..4]);
					pos = end;
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
						if (data.len < 2) {
							return error.UnexpectedDBMessage;
						}
						number_of_columns = std.mem.readIntBig(u16, data[0..2]);
						if (number_of_columns > state.result_oids.len) {
							// we have more columns than our self._query_state can handle, we
							// need to create a new QueryState specifically for this
							dyn_state = true;
							state = try QueryState.init(allocator, number_of_columns);
						}
						try state.from(number_of_columns, data);
					},
					else => return error.UnexpectedDBMessage,
				}
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
			try types.resultEncoding(state.result_oids[0..number_of_columns], buf);

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

			bind_complete: while (true) {
				const msg = try self.read();
				switch (msg.type) {
					'Z' => {}, // ReadyForquery
					'2' => break :bind_complete, // BindComplete
					else => return error.UnexpectedDBMessage,
				}
			}
		}

		return .{
			._state = state,
			._allocator = allocator,
			._dyn_state = dyn_state,
			._reader = &self._reader,
			.number_of_columns = number_of_columns,
		};
	}

	// Execute a query that does not return rows
	pub fn exec(self: *Conn, sql: []const u8, values: anytype) !?i64 {
		return self.execOpts(sql, values, .{});
	}

	pub fn execOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !?i64 {
		var buf = &self._buf;
		buf.reset();

		if (values.len == 0) {
			const simple_query = proto.Query{.sql = sql};
			try simple_query.write(buf);
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
		var response: bool = false;
		var affected: ?i64 = null;
		while (true) {
			const msg = try self.read();
			switch (msg.type) {
				'T' => {
					affected = 0;
					response = true;
				},
				'D' => affected = (affected orelse 0) + 1,
				'C' => {
					response = true;
					const cc = try proto.CommandComplete.parse(msg.data);
					affected = cc.rowsAffected();
				},
				'Z' => { // ready for query
					if (response) return affected;
					// else, must have come from before, keep going
				},
 				else => return error.UnexpectedDBMessage,
			}
		}
	}

	fn authSASL(self: *Conn, opts: StartupOpts, req: proto.AuthenticationRequest.SASL) !void {
		if (!req.scram_sha_256) {
			return error.UnusportedSASLMechanism;
		}
		var buf = &self._buf;

		var sasl = try lib.SASL.init(self._allocator);
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

	fn read(self: *Conn) !lib.Message {
		var reader = &self._reader;
		while (true) {
			const msg = try reader.next();
			switch (msg.type) {
				'N' => {}, // TODO: NoticeResponse
				'E' => return self.pgError(msg.data),
				else => return msg,
			}
		}
	}

	fn pgError(self: *Conn, data: []const u8) error{PG, OutOfMemory} {
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
};

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

test "Conn: parse error" {
	var c = t.connect(.{});
	defer c.deinit();
	try t.expectError(error.PG, c.query("selct 1", .{}));

	const err = c.err.?;
	try t.expectString("42601", err.code);
	try t.expectString("ERROR", err.severity);
	try t.expectString("syntax error at or near \"selct\"", err.message);
}

test "PG: type support" {
	var c = t.connect(.{});
	defer c.deinit();
	{
		const result = c.exec(\\
		\\ insert into all_types (
		\\   id,
		\\   col_int2, col_int4, col_int8, col_float4, col_float8,
		\\   col_bool, col_text, col_bytea
		\\ ) values (
		\\   $1,
		\\   $2, $3, $4, $5, $6,
		\\   $7, $8, $9
		\\ )
		, .{
			1,
			@as(i16, 382), @as(i32, -96534), @as(i64, 8983919283), @as(f32, 1.2345), @as(f64, -48832.3233231),
			true, "a text column", [_]u8{0, 0, 2, 255, 255, 255}
		});
		if (result) |affected| {
			try t.expectEqual(1, affected);
		} else |err| {
			try t.fail(c, err);
		}
	}

	var result = try c.query("select * from all_types where id = $1", .{1});
	defer result.deinit();

	const row = (try result.next()) orelse unreachable;
	try t.expectEqual(1, row.get(i32, 0));
	try t.expectEqual(382, row.get(i16, 1));
	try t.expectEqual(-96534, row.get(i32, 2));
	try t.expectEqual(8983919283, row.get(i64, 3));
	try t.expectEqual(1.2345, row.get(f32, 4));
	try t.expectEqual(-48832.3233231, row.get(f64, 5));
	try t.expectEqual(true, row.get(bool, 6));
	try t.expectString("a text column", row.get([]u8, 7));
	try t.expectSlice(u8, &.{0, 0, 2, 255, 255, 255}, row.get([]const u8, 8));
	try t.expectEqual(null, try result.next());
}

test "PG: null support" {
	var c = t.connect(.{});
	defer c.deinit();
	{
		const result = c.exec(\\
		\\ insert into all_types (id,
		\\   col_int2, col_int4, col_int8, col_float4, col_float8,
		\\   col_bool, col_text, col_bytea
		\\ ) values (
		\\   $1,
		\\   $2, $3, $4, $5, $6,
		\\   $7, $8, $9
		\\ )
		, .{
			2,
			null, null, null, null, null,
			null, null, null
		});
		if (result) |affected| {
			try t.expectEqual(1, affected);
		} else |err| {
			try t.fail(c, err);
		}
	}

	var result = try c.query("select * from all_types where id = $1", .{2});
	defer result.deinit();

	const row = (try result.next()) orelse unreachable;
	try t.expectEqual(null, row.get(?i16, 1));
	try t.expectEqual(null, row.get(?i32, 2));
	try t.expectEqual(null, row.get(?i64, 3));
	try t.expectEqual(null, row.get(?f32, 4));
	try t.expectEqual(null, row.get(?f64, 5));
	try t.expectEqual(null, row.get(?bool, 6));
	try t.expectEqual(null, row.get(?[]u8, 7));
	try t.expectEqual(null, row.get(?[]const u8, 8));
	try t.expectEqual(null, try result.next());
}
