const std = @import("std");
const lib = @import("lib.zig");
const Buffer = @import("buffer").Buffer;

const proto = lib.proto;
const types = lib.types;
const Pool = lib.Pool;
const Stmt = lib.Stmt;
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

// The underlying data for err
	_err_data: ?[]const u8,

	_stream: Stream,

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
		idle,

		// something bad happened
		fail,

		// we're doing a query
		query,

		// we're in a transaction
		transaction,
	};

	pub const Opts = struct {
		host: ?[]const u8 = null,
		port: ?u16 = null,
		unix_socket: ?[]const u8 = null,
		write_buffer: ?u16 = null,
		read_buffer: ?u16 = null,
		result_state_size: u16 = 32,
	};

	pub const QueryOpts = struct {
		timeout: ?u32 = null,
		column_names: bool = false,

		allocator: ?Allocator = null,
		// Whether a call to result.deinit() should automatically release the
		// connection back to the pool. Meant to be used internally by pool.query()
		// and the other pool utility wrappers, but applications might find it useful
		// to use in their own helpers
		release_conn: bool = false,
	};

	pub fn open(allocator: Allocator, opts: Opts) !Conn {
		const stream = blk: {
			if (opts.unix_socket) |path| {
				break :blk try std.net.connectUnixSocket(path);
			} else {
				const host = opts.host orelse "127.0.0.1";
				const port = opts.port orelse 5432;
				break :blk try std.net.tcpConnectToHost(allocator, host, port);
			}
		};
		errdefer stream.close();

		const buf = try Buffer.init(allocator, @min(opts.write_buffer orelse 2048, 128));
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
			._state = .idle,
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

	pub fn auth(self: *Conn, opts: lib.auth.Opts) !void {
		if (try lib.auth.auth(self._stream, &self._buf, &self._reader, opts)) |raw_pg_err| {
			return self.setErr(raw_pg_err);
		}

		while (true) {
			const msg = try self.read();
			switch (msg.type) {
				'Z' => return,
				'K' => {}, // TODO: BackendKeyData
				else => return self.unexpectedDBMessage(),
			}
		}
	}

	pub fn prepare(self: *Conn, sql: []const u8) !Stmt {
		return self.prepareOpts(sql, .{});
	}

	pub fn prepareOpts(self: *Conn, sql: []const u8, opts: QueryOpts) !Stmt {
		var stmt = try Stmt.init(self, opts);
		errdefer stmt.deinit();
		try stmt.prepare(sql);
		return stmt;
	}

	pub fn query(self: *Conn, sql: []const u8, values: anytype) !Result {
		return self.queryOpts(sql, values, .{});
	}

	pub fn queryOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !Result {
		if (self.canQuery() == false) {
			return error.ConnectionBusy;
		}

		var stmt = try Stmt.init(self, opts);
		errdefer stmt.deinit();

		try stmt.prepare(sql);

		inline for (values) |value| {
			try stmt.bind(value);
		}

		return stmt.execute();
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

		if (values.len == 0) {
			try self._reader.startFlow(opts.allocator, opts.timeout);
			defer self._reader.endFlow() catch {
				// this can only fail in extreme conditions (OOM) and it will only impact
				// the next query (and if the app is using the pool, the pool will try to
				// recover from this anyways)
				self._state = .fail;
			};
			const simple_query = proto.Query{.sql = sql};
			try simple_query.write(buf);
			// no longer idle, we're now in a query
			lib.metrics.query();
			self._state = .query;
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
 				else => return self.unexpectedDBMessage(),
			}
		}
	}

	pub fn begin(self: *Conn) !void {
		self._state = .transaction;
		_ = try self.execOpts("begin", .{}, .{});
	}

	pub fn commit(self: *Conn) !void {
		_ = try self.execOpts("commit", .{}, .{});
	}

	pub fn rollback(self: *Conn) !void {
		_ = try self.execOpts("rollback", .{}, .{});
	}

	// Should not be called directly
	pub fn read(self: *Conn) !lib.Message {
		var reader = &self._reader;
		while (true) {
			const msg = reader.next() catch |err| {
				self._state = .fail;
				return err;
			};
			switch (msg.type) {
				'Z' => {
					self._state = switch (msg.data[0]) {
						'I' => .idle,
						'T' => .transaction,
						'E' => .fail,
						else => unreachable,
					};
					return msg;
				},
				'S' => {}, // TODO: ParameterStatus,
				'N' => {}, // TODO: NoticeResponse
				'E' => return self.setErr(msg.data),
				else => return msg,
			}
		}
	}

	pub fn write(self: *Conn, data: []const u8) !void {
		self._stream.writeAll(data) catch |err| {
			self._state = .fail;
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

	pub fn unexpectedDBMessage(self: *Conn) error{UnexpectedDBMessage} {
		self._state = .fail;
		return error.UnexpectedDBMessage;
	}

	fn canQuery(self: *const Conn) bool {
		const state = self._state;
		if (state == .idle or state == .transaction) {
			return true;
		}
		return false;
	}

	// should not be called directly
	pub fn readyForQuery(self: *Conn) !void {
		const msg = try self.read();
		if (msg.type != 'Z') {
			return self.unexpectedDBMessage();
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
	defer t.reset();

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
		\\   col_timestamptz, col_timestamptz_arr,
		\\   col_json, col_json_arr,
		\\   col_jsonb, col_jsonb_arr,
		\\   col_char, col_char_arr,
		\\   col_charn, col_charn_arr,
		\\   col_cidr, col_cidr_arr,
		\\   col_inet, col_inet_arr,
		\\   col_macaddr, col_macaddr_arr,
		\\   col_macaddr8, col_macaddr8_arr
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
		\\   $34, $35,
		\\   $36, $37,
		\\   $38, $39,
		\\   $40, $41,
		\\   $42, $43
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
			"2023-10-23T15:33:13Z", [_][]const u8{"2010-02-10T08:22:07Z", "0003-04-05T06:07:08.123456"},
			"2024-11-23T16:34:14Z", [_][]const u8{"2011-03-11T09:23:05Z", "0002-03-04T05:06:02.0000991"},
			"{\"count\":1.3}", [_][]const u8{"[1,2,3]", "{\"rows\":[{\"a\": true}]}"},
			"{\"over\":9000}", [_][]const u8{"[true,false]", "{\"cols\":[{\"z\": 0.003}]}"},
			79, [_]u8{'1', 'z', '!'},
			"Teg", [_][]const u8{&.{78, 82}, "hi"},
			"192.168.100.128/25", [_][]const u8{"10.1.2", "2001:4f8:3:ba::/64"},
			"::ffff:1.2.3.0/120", [_][]const u8{"127.0.0.1/32", "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128"},
			"08:00:2b:01:02:03", [_][]const u8{"08002b:010203", "0800-2b01-0204"},
			"09:01:3b:21:21:03:04:05", [_][]const u8{"ffeeddccbbaa9988", "01-02-03-04-05-06-07-09"},
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
		\\   col_timestamptz, col_timestamptz_arr,
		\\   col_json, col_json_arr,
		\\   col_jsonb, col_jsonb_arr,
		\\   col_char, col_char_arr,
		\\   col_charn, col_charn_arr,
		\\   col_cidr, col_cidr_arr,
		\\   col_inet, col_inet_arr,
		\\   col_macaddr, col_macaddr_arr,
		\\   col_macaddr8, col_macaddr8_arr
		\\ from all_types where id = $1
	, .{1});
	defer result.deinit();

	// used for our arrays
	const aa = t.arena.allocator();

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
		const arr = try row.iterator(types.Numeric, 22).alloc(aa);
		try t.expectEqual(5, arr.len);
		try expectNumeric(arr[0], "0.0");
		try expectNumeric(arr[1], "-1.1");
		try expectNumeric(arr[2], "nan");
		try expectNumeric(arr[3], "inf");
		try expectNumeric(arr[4], "12345.000101");
	}

	{
		//timestamp, timestamp[]
		try t.expectEqual(1698075193000000, row.get(i64, 23));
		try t.expectSlice(i64, &.{1265790127000000, -62064381171876544}, try row.iterator(i64, 24).alloc(aa));
	}

	{
		//timestamptz, timestamptz[]
		try t.expectEqual(1732379654000000, row.get(i64, 25));
		try t.expectSlice(i64, &.{1299835385000000, -62098685637999901}, try row.iterator(i64, 26).alloc(aa));
	}

	{
		// json, json[]
		try t.expectString("{\"count\":1.3}", row.get([]u8, 27));
		const arr = try row.iterator([]const u8, 28).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("[1,2,3]", arr[0]);
		try t.expectString("{\"rows\":[{\"a\": true}]}", arr[1]);
	}

	{
		// jsonb, jsonb[]
		try t.expectString("{\"over\": 9000}", row.get([]u8, 29));
		const arr = try row.iterator([]const u8, 30).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("[true, false]", arr[0]);
		try t.expectString("{\"cols\": [{\"z\": 0.003}]}", arr[1]);
	}

	{
		// char, char[]
		try t.expectEqual(79, row.get(u8, 31));
		const arr = try row.iterator(u8, 32).alloc(aa);
		try t.expectEqual(3, arr.len);
		try t.expectEqual('1', arr[0]);
		try t.expectEqual('z', arr[1]);
		try t.expectEqual('!', arr[2]);
	}

	{
		// charn, charn[]
		try t.expectString("Teg", row.get([]u8, 33));
		const arr = try row.iterator([]u8, 34).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("NR", arr[0]);
		try t.expectString("hi", arr[1]);
	}

	{
		// cidr, cidr[]
		const cidr = row.get(types.Cidr, 35);
		try t.expectEqual(25, cidr.netmask);
		try t.expectEqual(.v4, cidr.family);
		try t.expectString(&.{192, 168, 100, 128}, cidr.address);

		const arr = try row.iterator(types.Cidr, 36).alloc(aa);
		try t.expectEqual(2, arr.len);

		try t.expectEqual(24, arr[0].netmask);
		try t.expectEqual(.v4, arr[0].family);
		try t.expectString(&.{10, 1, 2, 0}, arr[0].address);

		try t.expectEqual(64, arr[1].netmask);
		try t.expectEqual(.v6, arr[1].family);
		try t.expectSlice(u8, &.{32, 1, 4, 248, 0, 3, 0, 186, 0, 0, 0, 0, 0, 0, 0, 0}, arr[1].address);
	}

	{
		// inet, inet[]
		const inet = row.get(types.Cidr, 37);
		try t.expectEqual(120, inet.netmask);
		try t.expectEqual(.v6, inet.family);
		try t.expectString(&.{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 1, 2, 3, 0}, inet.address);

		const arr = try row.iterator(types.Cidr, 38).alloc(aa);
		try t.expectEqual(2, arr.len);

		try t.expectEqual(32, arr[0].netmask);
		try t.expectEqual(.v4, arr[0].family);
		try t.expectString(&.{127, 0, 0, 1}, arr[0].address);

		try t.expectEqual(128, arr[1].netmask);
		try t.expectEqual(.v6, arr[1].family);
		try t.expectSlice(u8, &.{32, 1, 4, 248, 0, 3, 0, 186, 2, 224, 129, 255, 254, 34, 209, 241}, arr[1].address);
	}

	{
		// macaddr, macaddr[]
		try t.expectSlice(u8, &.{8, 0, 43, 1, 2, 3}, row.get([]u8, 39));

		const arr = try row.iterator([]u8, 40).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectSlice(u8, &.{8, 0, 43, 1, 2, 3}, arr[0]);
		try t.expectSlice(u8, &.{8, 0, 43, 1, 2, 4}, arr[1]);
	}

	{
		// macaddr8, macaddr8[]
		try t.expectSlice(u8, &.{9, 1, 59, 33, 33, 3, 4, 5}, row.get([]u8, 41));

		const arr = try row.iterator([]u8, 42).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectSlice(u8, &.{255, 238, 221, 204, 187, 170, 153, 136}, arr[0]);
		try t.expectSlice(u8, &.{1, 2, 3, 4, 5, 6, 7, 9}, arr[1]);
	}

	try t.expectEqual(null, try result.next());
}

// For ambiguous types, the above "type support" test is using the text-representation
// This test will use the binary representation of each of the ambiguous types
test "PG: binary support" {
	defer t.reset();

	var c = t.connect(.{});
	defer c.deinit();

	{
		const result = c.exec(\\
		\\ insert into all_types (
		\\   id,
		\\   col_uuid, col_uuid_arr,
		\\   col_timestamp, col_timestamp_arr,
		\\   col_timestamptz, col_timestamptz_arr,
		\\   col_numeric, col_numeric_arr,
		\\   col_macaddr, col_macaddr_arr,
		\\   col_macaddr8, col_macaddr8_arr
		\\ ) values (
		\\   $1,
		\\   $2, $3,
		\\   $4, $5,
		\\   $6, $7,
		\\   $8, $9,
		\\   $10, $11,
		\\   $12, $13
		\\ )
		, .{
			2,
			&[_]u8{142, 243, 93, 100, 249, 159, 77, 126, 167, 54, 150, 204, 170, 222, 98, 124}, [_][]const u8{&.{53, 140, 59, 37, 1, 148, 72, 139, 130, 197, 181, 40, 44, 109, 127, 165}, &.{57, 203, 218, 97, 37, 38, 70, 107, 182, 116, 24, 125, 236, 123, 117, 247}},
			169804639500713, [_]i64{169804639500713, -94668480000000},
			169804639500714, [_]i64{169804639500714, -94668480000001},
			"-394956.2221", [_][]const u8{"1.0008", "-987.110", "-inf"},
			&[_]u8{1,2,3,4,5,6}, [_][]const u8{&.{0, 1, 0, 2, 0, 3}, &.{255, 0, 254, 1, 253, 2}},
			&[_]u8{1,2,3,4,5,6,7,8}, [_][]const u8{&.{0, 1, 0, 2, 0, 3, 4, 0}, &.{255, 0, 254, 1, 253, 2, 3, 252}},
		});
		if (result) |affected| {
			try t.expectEqual(1, affected);
		} else |err| {
			try t.fail(c, err);
		}
	}

	var result = try c.query(
		\\ select
		\\   col_uuid, col_uuid_arr,
		\\   col_timestamp, col_timestamp_arr,
		\\   col_timestamptz, col_timestamptz_arr,
		\\   col_numeric, col_numeric_arr,
		\\   col_macaddr, col_macaddr_arr,
		\\   col_macaddr8, col_macaddr8_arr
		\\ from all_types where id = $1
	, .{2});
	defer result.deinit();

	// used for our arrays
	const aa = t.arena.allocator();

	const row = (try result.next()) orelse unreachable;

	{
		//uuid, uuid[]
		try t.expectString("8ef35d64-f99f-4d7e-a736-96ccaade627c", &(try types.UUID.toString(row.get([]u8, 0))));

		const arr = try row.iterator([]const u8, 1).alloc(aa);
		try t.expectEqual(2, arr.len);
		try t.expectString("358c3b25-0194-488b-82c5-b5282c6d7fa5", &(try types.UUID.toString(arr[0])));
		try t.expectString("39cbda61-2526-466b-b674-187dec7b75f7", &(try types.UUID.toString(arr[1])));
	}

	{
		//timestamp, timestamp[]
		try t.expectEqual(169804639500713, row.get(i64, 2));
		try t.expectSlice(i64, &.{169804639500713, -94668480000000}, try row.iterator(i64, 3).alloc(aa));
	}

	{
		//timestamptz, timestamptz[]
		try t.expectEqual(169804639500714, row.get(i64, 4));
		try t.expectSlice(i64, &.{169804639500714, -94668480000001}, try row.iterator(i64, 5).alloc(aa));
	}

	{
		//numeric, numeric[]
		try t.expectEqual(-394956.2221, row.get(f64, 6));
		try t.expectSlice(f64, &.{1.0008, -987.110, -std.math.inf(f64)}, try row.iterator(f64, 7).alloc(aa));
	}

	{
		//macaddr, macaddr[]
		try t.expectSlice(u8, &.{1,2,3,4,5,6}, row.get([]u8, 8));
		const arr = try row.iterator([]u8, 9).alloc(aa);
		try t.expectSlice(u8, &.{0, 1, 0, 2, 0, 3}, arr[0]);
		try t.expectSlice(u8, &.{255, 0, 254, 1, 253, 2}, arr[1]);
	}

	{
		//macaddr8, macaddr8[]
		try t.expectSlice(u8, &.{1,2,3,4,5,6,7,8}, row.get([]u8, 10));
		const arr = try row.iterator([]u8, 11).alloc(aa);
		try t.expectSlice(u8, &.{0, 1, 0, 2, 0, 3, 4, 0}, arr[0]);
		try t.expectSlice(u8, &.{255, 0, 254, 1, 253, 2, 3, 252}, arr[1]);
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
			3,
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
	, .{3});
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
		, .{4, DummyStruct{.id = 1, .name = "Leto"}, &DummyStruct{.id = 2, .name = "Ghanima"}});

		if (result) |affected| {
			try t.expectEqual(1, affected);
		} else |err| {
			try t.fail(c, err);
		}
	}

	var result = try c.query("select col_json, col_jsonb from all_types where id = $1", .{4});
	defer result.deinit();

	const row = (try result.next()) orelse unreachable;
	try t.expectString("{\"id\":1,\"name\":\"Leto\"}", row.get([]u8, 0));
	try t.expectString("{\"id\": 2, \"name\": \"Ghanima\"}", row.get(?[]const u8, 1).?);
}

test "Conn: prepare" {
	var c = t.connect(.{});
	defer c.deinit();

	var stmt = try c.prepare("select $1::int where $2");
	try stmt.bind(938);
	try stmt.bind(true);

	var result = try stmt.execute();
	defer result.deinit();

	var row = (try result.next()) orelse unreachable;
	try t.expectEqual(938, row.get(i32, 0));

	try t.expectEqual(null, try result.next());
}

test "PG: row" {
	var c = t.connect(.{});
	defer c.deinit();

	const r1 = try c.row("select 1 where $1", .{false});
	try t.expectEqual(null, r1);

	var r2 = (try c.row("select 2 where $1", .{true})) orelse unreachable;
	try t.expectEqual(2, r2.get(i32, 0));
	try r2.deinit();

	// make sure the conn is still valid after a successful row
	var r3 = (try c.row("select $1::int where $2", .{3, true})) orelse unreachable;
	try t.expectEqual(3, r3.get(i32, 0));
	try r3.deinit();

	// make sure the conn is still valid after MoreThanOneRow error
	var r4 = (try c.row("select $1::text where $2", .{"hi", true})) orelse unreachable;
	try t.expectString("hi", r4.get([]u8, 0));
	try r4.deinit();
}

test "PG: begin/commit" {
	var c = t.connect(.{});
	defer c.deinit();

	try c.begin();
	_ = try c.exec("delete from simple_table", .{});
	_ = try c.exec("insert into simple_table values ($1)", .{"begin_commit"});
	try c.commit();

	var row = (try c.row("select value from simple_table", .{})).?;
	defer row.deinit() catch {};

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
		\\ values (5, $1, $2, $3, $4)
	, .{DummyEnum.val1, &[_]DummyEnum{DummyEnum.val1, DummyEnum.val2}, DummyEnum.val2, [_]DummyEnum{DummyEnum.val2, DummyEnum.val1}});

	var row = (try c.row(
		\\ select col_enum, col_text, col_enum_arr, col_text_arr
		\\ from all_types
		\\ where id = 5
	, .{})) orelse unreachable;
	defer row.deinit() catch {};

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
	defer t.reset();

	var c = t.connect(.{});
	defer c.deinit();

	{
		// read
		var row = (try c.row(
			\\ select 'nan'::numeric, '+Inf'::numeric, '-Inf'::numeric,
			\\ 0::numeric, 0.0::numeric, -0.00009::numeric, -999999.888880::numeric,
			\\ 0.000008, 999999.888807::numeric, 123456.78901234::numeric(14, 8)
		, .{})).?;
		defer row.deinit() catch {};

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
		var row = (try c.row(
			\\ select
			\\   $1::numeric, $2::numeric, $3::numeric,
			\\   $4::numeric, $5::numeric, $6::numeric,
			\\   $7::numeric, $8::numeric, $9::numeric,
			\\   $10::numeric, $11::numeric, $12::numeric,
			\\   $13::numeric, $14::numeric, $15::numeric,
			\\   $16::numeric, $17::numeric, $18::numeric,
			\\   $19::numeric, $20::numeric, $21::numeric,
			\\   $22::numeric, $23::numeric, $24::numeric,
			\\   $25::numeric, $26::numeric, $27::numeric[]
		, .{
			-0.00089891, 939293122.0001101, "-123.4560991",
			std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64),
			std.math.nan(f32), std.math.inf(f32), -std.math.inf(f32),
			1.1, 12.98, 123.987,
			1234.9876, 12345.98765, 123456.987654,
			1234567.9876543, 12345678.98765432, 123456789.987654321,
			@as(f64, 0), @as(f64, 1), 0,
			1, 999999999.9999999, @as(f64, 999999999.9999999),
			-999999999.9999999, @as(f64, -999999999.9999999),
			&[_][]const u8{"1.1", "-0.0034"}
		})).?;
		defer row.deinit() catch {};

		{
			// test the pg.Numeric fields
			const numeric = row.get(types.Numeric, 1);
			try t.expectEqual(939293122.0001101, numeric.toFloat());
			try t.expectEqual(2, numeric.weight);
			try t.expectEqual(.positive, numeric.sign);
			try t.expectEqual(7, numeric.scale);
			try t.expectSlice(u8, &.{0, 9, 15, 89, 12, 50, 0, 1, 3, 242}, numeric.digits);
		}

		try expectNumeric(row.get(types.Numeric, 0), "-0.00089891");
		try expectNumeric(row.get(types.Numeric, 1), "939293122.0001101");
		try expectNumeric(row.get(types.Numeric, 2), "-123.4560991");

		try expectNumeric(row.get(types.Numeric, 3), "nan");
		try expectNumeric(row.get(types.Numeric, 4), "inf");
		try expectNumeric(row.get(types.Numeric, 5), "-inf");

		try expectNumeric(row.get(types.Numeric, 6), "nan");
		try expectNumeric(row.get(types.Numeric, 7), "inf");
		try expectNumeric(row.get(types.Numeric, 8), "-inf");

		try expectNumeric(row.get(types.Numeric, 9), "1.1");
		try expectNumeric(row.get(types.Numeric, 10), "12.98");
		try expectNumeric(row.get(types.Numeric, 11), "123.987");
		try expectNumeric(row.get(types.Numeric, 12), "1234.9876");
		try expectNumeric(row.get(types.Numeric, 13), "12345.98765");
		try expectNumeric(row.get(types.Numeric, 14), "123456.987654");
		try expectNumeric(row.get(types.Numeric, 15), "1234567.9876543");
		try expectNumeric(row.get(types.Numeric, 16), "12345678.98765432");
		try expectNumeric(row.get(types.Numeric, 17), "123456789.987654321");
		try expectNumeric(row.get(types.Numeric, 18), "0.0");
		try expectNumeric(row.get(types.Numeric, 19), "1.0");
		try expectNumeric(row.get(types.Numeric, 20), "0.0");
		try expectNumeric(row.get(types.Numeric, 21), "1.0");
		try expectNumeric(row.get(types.Numeric, 22), "999999999.9999999");
		try expectNumeric(row.get(types.Numeric, 23), "999999999.9999999");
		try expectNumeric(row.get(types.Numeric, 24), "-999999999.9999999");
		try expectNumeric(row.get(types.Numeric, 25), "-999999999.9999999");


		const arr = try row.iterator(types.Numeric, 26).alloc(t.arena.allocator());
		try t.expectEqual(2, arr.len);
		try t.expectEqual(1.1, arr[0].toFloat());
		try t.expectDelta(-0.0034, arr[1].toFloat(), 0.00000001);
	}
}

// char array encoding is a little special, so let's test variants
test "PG: char" {
	defer t.reset();

	var c = t.connect(.{});
	defer c.deinit();

	// read
	var row = (try c.row(
		\\ select $1::char[], $2::char[], $3::char[], $4::char[]
	, .{&[_]u8{','}, &[_]u8{',', '"'}, &[_]u8{'\\', 'a', ' '}, &[_]u8{'z', '@'}})).?;
	defer row.deinit() catch {};

	// used for our arrays
	const aa = t.arena.allocator();

	try t.expectSlice(u8, &.{','}, try row.iterator(u8, 0).alloc(aa));
	try t.expectSlice(u8, &.{',', '"'}, try row.iterator(u8, 1).alloc(aa));
	try t.expectSlice(u8, &.{'\\', 'a', ' '}, try row.iterator(u8, 2).alloc(aa));
	try t.expectSlice(u8, &.{'z', '@'}, try row.iterator(u8, 3).alloc(aa));
}

test "PG: bind []const u8" {
	defer t.reset();

	var c = t.connect(.{});
	defer c.deinit();
	const value: []const u8 = "hello";

	{
		const result = c.exec("insert into all_types (id, col_text) values ($1, $2)", .{6, value});
		if (result) |affected| {
			try t.expectEqual(1, affected);
		} else |err| {
			try t.fail(c, err);
		}
	}

	var result = try c.query("select id, col_text from all_types where id = $1", .{6});
	defer result.deinit();

	const row = (try result.next()) orelse unreachable;
	try t.expectEqual(6, row.get(i32, 0));
	try t.expectString("hello", row.get([]u8, 1));
}

test "PG: isUnique" {
	defer t.reset();

	var c = t.connect(.{});
	defer c.deinit();

	{
		try t.expectError(error.PG, c.exec("insert into all_types (id, id) values ($1)", .{7, null}));
		try t.expectEqual(false, c.err.?.isUnique());
	}

	{
		_ = try c.exec("insert into all_types (id) values ($1)", .{7});
		_ = try t.expectError(error.PG, c.exec("insert into all_types (id) values ($1)", .{7}));
		try t.expectEqual(true, c.err.?.isUnique());
	}
}

test "PG: large read" {
	var c = t.connect(.{.read_buffer = 500});
	defer c.deinit();

	{
		// want this to be larger than our read_buffer
		var rows = try c.query("select $1::text", .{"!" ** 1000});
		defer rows.deinit();

		const row = (try rows.next()).?;
		try t.expectString("!" ** 1000, row.get([]u8, 0));
		try t.expectEqual(null, try rows.next());
	}

	{
		// with a row
		var row = (try c.row("select $1::text", .{"z" ** 1000})).?;
		defer row.deinit() catch {};
		try t.expectString("z" ** 1000, row.get([]u8, 0));
	}
}

test "PG: Record" {
	var c = t.connect(.{});
	defer c.deinit();

	{
		var row = (try c.row("select row(9001, 'hello'::text)", .{})).?;
		defer row.deinit() catch {};

		var record = row.record(0);
		try t.expectEqual(2, record.number_of_columns);
		try t.expectEqual(9001, record.next(i32));
		try t.expectString("hello", record.next([]const u8));
	}

	{
		var row = (try c.row("select row(null)", .{})).?;
		defer row.deinit() catch {};

		var record = row.record(0);
		try t.expectEqual(1, record.number_of_columns);
		try t.expectEqual(null, record.next(?i32));
	}
}

test "Conn: application_name" {
	var conn = try Conn.open(t.allocator, .{});
	defer conn.deinit();
	try conn.auth(.{
		.username = "pgz_user_clear",
		.password = "pgz_user_clear_pw",
		.database = "postgres",
		.application_name = "pg_zig_test",
	});

	var row = (try conn.row("show application_name", .{})) orelse unreachable;
	defer row.deinit() catch {};

	try t.expectString("pg_zig_test", row.get([]const u8, 0));
}

fn expectNumeric(numeric: types.Numeric, expected: []const u8) !void {
	var str_buf: [50]u8 = undefined;
	try t.expectString(expected, numeric.toString(&str_buf));

	const a = try t.allocator.alloc(u8, numeric.estimatedStringLen());
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
