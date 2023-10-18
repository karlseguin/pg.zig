const std = @import("std");
const lib = @import("lib.zig");

const types = lib.types;
const proto = lib.proto;
const Reader = lib.Reader;
const QueryState = lib.QueryState;
const Allocator = std.mem.Allocator;

pub const Result = struct {
	_reader: *Reader,
	_allocator: Allocator,

	// when true, then _state was dynamically allocated and we're responsible for it
	_dyn_state: bool,
	_state: QueryState,

	// the underlying data for err
	_err_data: ?[]const u8 = null,
	err: ?proto.Error = null,
	number_of_columns: usize,

	pub fn deinit(self: Result) void {
		const allocator = self._allocator;
		if (self._dyn_state) {
			self._state.deinit(allocator);
		}

		if (self._err_data) |err_data| {
			allocator.free(err_data);
		}
	}

	// Caller should typically call next() until null is returned.
	// But in some cases, that might not be desirable. So they can
	// "drain" to empty the rest of the result.
	// I don't want to do this implictly in deinit because it can fail
	// and returning an error union in deinit is a pain for the caller.
	pub fn drain(self: *Result) !void {
		while (true) {
			const msg = try self.read();
			switch (msg.type) {
				'C' => {}, // CommandComplete
				'D' => {}, // DataRow
				'Z' => return, // ready for query
				else => return error.UnexpectedDBMessage,
			}
		}
	}

	pub fn next(self: *Result) !?Row {
		const msg = try self.read();
		switch (msg.type) {
			'D' => {
				const data = msg.data;
				const state = self._state;

				// Since our Row API gets data by column #, we need translate the column
				// # to a slice within msg.data. We could do this on the fly within Row,
				// but creating this mapping up front simplifies things and, in normal
				// cases, performs best. "Normal case" here assumes that the client app
				// is going to fetch most/all columns.

				// first column starts at position 2
				var offset: usize = 2;
				var values = state.values;
				for (0..self.number_of_columns) |i| {
					const data_start = offset + 4;
					const length = std.mem.readIntBig(i32, data[offset..data_start][0..4]);
					if (length == -1) {
						values[i].is_null = true;
					} else {
						const data_end = data_start + @as(usize, @intCast(length));
						var entry = &values[i];
						entry.is_null = false;
						entry.data = data[data_start..data_end];
						offset = data_end;
					}
				}

				return .{
					.values = values,
					.oids = state.result_oids,
				};
			},
			'C' => return null, // CommandComplete
			else => return error.UnexpectedDBMessage,
		}
	}

	fn read(self: *Result) !lib.Message {
		var reader = self._reader;
		while (true) {
			const msg = try reader.next();
			switch (msg.type) {
				'E' => return self.pgError(msg.data),
				else => return msg,
			}
		}
	}

	fn pgError(self: *Result, data: []const u8) error{PG, OutOfMemory} {
		const allocator = self._allocator;

		// The proto.Error that we're about to create is going to reference data.
		// But data is owned by our Reader and its lifetime doesn't necessarily match
		// what we want here. So we're going to dupe it and make the result own
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

pub const Row = struct {
	oids: []i32,
	values: []QueryState.Value,

	pub fn getInt16(self: *const Row, col: usize) i16 {
		return types.Int16.decode(self.values[col].data);
	}

	pub fn getNullInt16(self: *const Row, col: usize) ?i16 {
		const value = self.values[col];
		if (value.is_null) return null;
		return types.Int16.decode(value.data);
	}

	pub fn getInt32(self: *const Row, col: usize) i32 {
		return types.Int32.decode(self.values[col].data);
	}

	pub fn getNullInt32(self: *const Row, col: usize) ?i32 {
		const value = self.values[col];
		if (value.is_null) return null;
		return types.Int32.decode(value.data);
	}

	pub fn getInt64(self: *const Row, col: usize) i64 {
		return types.Int64.decode(self.values[col].data);
	}

	pub fn getNullInt64(self: *const Row, col: usize) ?i64 {
		const value = self.values[col];
		if (value.is_null) return null;
		return types.Int64.decode(value.data);
	}

	pub fn getFloat32(self: *const Row, col: usize) f32 {
		return types.Float32.decode(self.values[col].data);
	}

	pub fn getNullFloat32(self: *const Row, col: usize) ?f32 {
		const value = self.values[col];
		if (value.is_null) return null;
		return types.Float32.decode(value.data);
	}

	pub fn getFloat64(self: *const Row, col: usize) f64 {
		return types.Float64.decode(self.values[col].data);
	}

	pub fn getNullFloat64(self: *const Row, col: usize) ?f64 {
		const value = self.values[col];
		if (value.is_null) return null;
		return types.Float64.decode(value.data);
	}

	pub fn getBool(self: *const Row, col: usize) bool {
		return types.Bool.decode(self.values[col].data);
	}

	pub fn getNullBool(self: *const Row, col: usize) ?bool {
		const value = self.values[col];
		if (value.is_null) return null;
		return types.Bool.decode(value.data);
	}

	pub fn getString(self: *const Row, col: usize) []const u8 {
		return types.String.decode(self.values[col].data);
	}

	pub fn getNullString(self: *const Row, col: usize) ?[]const u8 {
		const value = self.values[col];
		if (value.is_null) return null;
		return types.String.decode(value.data);
	}
};

const t = lib.testing;
test "Result: ints" {
	var c = t.connect(.{});
	defer c.deinit();
	const sql = "select $1::smallint, $2::int, $3::bigint";

	{
		// int max
		var result = try c.query(sql, .{@as(i16, 32767), @as(i32, 2147483647), @as(i64, 9223372036854775807)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(32767, row.getInt16(0));
		try t.expectEqual(2147483647, row.getInt32(1));
		try t.expectEqual(9223372036854775807, row.getInt64(2));

		try t.expectEqual(32767, row.getNullInt16(0));
		try t.expectEqual(2147483647, row.getNullInt32(1));
		try t.expectEqual(9223372036854775807, row.getNullInt64(2));

		try t.expectEqual(null, result.next());
	}

	{
		// int min
		var result = try c.query(sql, .{@as(i16, -32768), @as(i32, -2147483648), @as(i64, -9223372036854775808)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(-32768, row.getInt16(0));
		try t.expectEqual(-2147483648, row.getInt32(1));
		try t.expectEqual(-9223372036854775808, row.getInt64(2));
		try result.drain();
	}

	{
		// int null
		var result = try c.query(sql, .{null, null, null});
		defer result.deinit();
		defer result.drain() catch unreachable;
		const row = (try result.next()).?;
		try t.expectEqual(null, row.getNullInt16(0));
		try t.expectEqual(null, row.getNullInt32(1));
		try t.expectEqual(null, row.getNullInt64(2));

	}

	{
		// uint within limit
		var result = try c.query(sql, .{@as(u16, 32767), @as(u32, 2147483647), @as(u64, 9223372036854775807)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(32767, row.getInt16(0));
		try t.expectEqual(2147483647, row.getInt32(1));
		try t.expectEqual(9223372036854775807, row.getInt64(2));

		try t.expectEqual(32767, row.getNullInt16(0));
		try t.expectEqual(2147483647, row.getNullInt32(1));
		try t.expectEqual(9223372036854775807, row.getNullInt64(2));
		try result.drain();
	}

	{
		// u16 outside of limit
		try t.expectError(error.UnsignedIntWouldBeTruncated, c.query(sql, .{@as(u16, 32768), @as(u32, 0), @as(u64, 0)}));
		// u32 outside of limit
		try t.expectError(error.UnsignedIntWouldBeTruncated, c.query(sql, .{@as(u16, 0), @as(u32, 2147483648), @as(u64, 0)}));
		// u64 outside of limit
		try t.expectError(error.UnsignedIntWouldBeTruncated, c.query(sql, .{@as(u16, 0), @as(u32, 0), @as(u64, 9223372036854775808)}));
	}
}

test "Result: floats" {
	var c = t.connect(.{});
	defer c.deinit();
	const sql = "select $1::float4, $2::float8";

	{
		// positive float
		var result = try c.query(sql, .{@as(f32, 1.23456), @as(f64, 1093.229183)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(1.23456, row.getFloat32(0));
		try t.expectEqual(1093.229183, row.getFloat64(1));

		try t.expectEqual(1.23456, row.getNullFloat32(0));
		try t.expectEqual(1093.229183, row.getNullFloat64(1));

		try t.expectEqual(null, result.next());
	}

	{
		// negative float
		var result = try c.query(sql, .{@as(f32, -392.31), @as(f64, -99991.99992)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(-392.31, row.getFloat32(0));
		try t.expectEqual(-99991.99992, row.getFloat64(1));
		try t.expectEqual(null, result.next());
	}

	{
		// null float
		var result = try c.query(sql, .{null, null});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(null, row.getNullFloat32(0));
		try t.expectEqual(null, row.getNullFloat64(1));
		try t.expectEqual(null, result.next());
	}
}

test "Result: bool" {
	var c = t.connect(.{});
	defer c.deinit();
	const sql = "select $1::bool";

	{
		// true
		var result = try c.query(sql, .{true});
		defer result.deinit();
		defer result.drain() catch unreachable;
		const row = (try result.next()).?;
		try t.expectEqual(true, row.getBool(0));
		try t.expectEqual(true, row.getNullBool(0));
		try t.expectEqual(null, result.next());
	}

	{
		// false
		var result = try c.query(sql, .{false});
		defer result.deinit();
		defer result.drain() catch unreachable;
		const row = (try result.next()).?;
		try t.expectEqual(false, row.getBool(0));
		try t.expectEqual(false, row.getNullBool(0));
		try t.expectEqual(null, result.next());
	}

	{
		// null
		var result = try c.query(sql, .{null});
		defer result.deinit();
		defer result.drain() catch unreachable;
		const row = (try result.next()).?;
		try t.expectEqual(null, row.getNullBool(0));
		try t.expectEqual(null, result.next());
	}
}

test "Result: test and bytea" {
	var c = t.connect(.{});
	defer c.deinit();
	const sql = "select $1::text, $2::bytea";

	{
		// empty
		var result = try c.query(sql, .{"", ""});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectString("", row.getString(0));
		try t.expectString("", row.getNullString(0).?);
		try t.expectString("", row.getString(1));
		try t.expectString("", row.getNullString(1).?);
		try result.drain();
	}

	{
		// not empty
		var result = try c.query(sql, .{"it's over 9000!!!", "i will Not fear"});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectString("it's over 9000!!!", row.getString(0));
		try t.expectString("it's over 9000!!!", row.getNullString(0).?);
		try t.expectString("i will Not fear", row.getString(1));
		try t.expectString("i will Not fear", row.getNullString(1).?);
		try result.drain();
	}

	{
		// as an array
		var result = try c.query(sql, .{[_]u8{'a', 'c', 'b'}, [_]u8{'z', 'z', '3'}});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectString("acb", row.getString(0));
		try t.expectString("acb", row.getNullString(0).?);
		try t.expectString("zz3", row.getString(1));
		try t.expectString("zz3", row.getNullString(1).?);
		try result.drain();
	}

	{
		// as a slice
		var s1 = try t.allocator.alloc(u8, 4);
		defer t.allocator.free(s1);
		@memcpy(s1, "Leto");

		var s2 = try t.allocator.alloc(u8, 7);
		defer t.allocator.free(s2);
		@memcpy(s2, "Ghanima");
		var result = try c.query(sql, .{s1, s2});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectString("Leto", row.getString(0));
		try t.expectString("Leto", row.getNullString(0).?);
		try t.expectString("Ghanima", row.getString(1));
		try t.expectString("Ghanima", row.getNullString(1).?);
		try result.drain();
	}

	{
		// null
		var result = try c.query(sql, .{null, null});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(null, row.getNullString(0));
		try t.expectEqual(null, row.getNullString(1));
		try result.drain();
	}
}

test "Result: optional" {
	var c = t.connect(.{});
	defer c.deinit();
	const sql = "select $1::int, $2::int";

	{
		// int max
		var result = try c.query(sql, .{@as(?i32, 321), @as(?i32, null)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(321, row.getInt32(0));

		try t.expectEqual(321, row.getNullInt32(0));
		try t.expectEqual(null, row.getNullInt32(1));
		try t.expectEqual(null, result.next());
	}
}
