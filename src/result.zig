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
	values: []QueryState.Value,

	pub fn get(self: *const Row, comptime T: type, col: usize) ScalarReturnType(T) {
		const value = self.values[col];
		const TT = switch (@typeInfo(T)) {
			.Optional => |opt| blk: {
				if (value.is_null) return null;
				break :blk opt.child;
			},
			else => T,
		};

		const data = value.data;
		switch (TT) {
			i16 => return types.Int16.decode(data),
			i32 => return types.Int32.decode(data),
			i64 => return types.Int64.decode(data),
			f32 => return types.Float32.decode(data),
			f64 => return types.Float64.decode(data),
			bool => return types.Bool.decode(data),
			[]u8, []const u8 => return types.String.decode(data),
			else => compileHaltGetError(T),
		}
	}

	fn ScalarReturnType(comptime T: type) type {
		return switch (T) {
			[]u8 => []const u8,
			?[]u8 => ?[]const u8,
			else => T,
		};
	}

	pub fn getIterator(self: *const Row, comptime T: type, col: usize) IteratorReturnType(T) {
		const value = self.values[col];
		const TT = switch (@typeInfo(T)) {
			.Optional => |opt| blk: {
				if (value.is_null) return null;
				break :blk opt.child;
			},
			else => T,
		};

		const decoder = switch (TT) {
			i16 => types.Int16.decode,
			i32 => types.Int32.decode,
			i64 => types.Int64.decode,
			f32 => types.Float32.decode,
			f64 => types.Float64.decode,
			else => compileHaltGetError(TT),
		};

		const data = value.data;

		if (data.len == 12) {
			// we have an empty
			return .{
				.len = 0,
				._pos = 0,
				._data = &[_]u8{},
				._decoder = decoder,
			};
		}

		// minimum size for 1 empty array
		std.debug.assert(data.len >= 20);
		const dimensions = std.mem.readIntBig(i32, data[0..4]);
		std.debug.assert(dimensions == 1);

		const has_nulls = std.mem.readIntBig(i32, data[4..8][0..4]);
		std.debug.assert(has_nulls == 0);

		// const oid = std.mem.readIntBig(i32, data[8..12][0..4]);
		const len = std.mem.readIntBig(i32, data[12..16][0..4]);
		// const lower_bound = std.mem.readIntBig(i32, data[16..20][0..4]);



		return .{
			.len = @intCast(len),
			._pos = 0,
			._data = data[20..],
			._decoder = decoder,
		};
	}

	fn IteratorReturnType(comptime T: type) type {
		return switch (T) {
			[]u8 => Iterator([]const u8),
			?[]u8 => ?Iterator([]const u8),
			else => {
				if (std.meta.activeTag(@typeInfo(T)) == .Optional) {
					return ?Iterator(T);
				}
				return Iterator(T);
			}
		};
	}
};

pub fn Iterator(comptime T: type) type {
	return struct {
		len: usize,
		_pos: usize,
		_data: []const u8,
		_decoder: *const fn(data: []const u8) T,

		const Self = @This();

		pub fn next(self: *Self) ?T {
			const pos = self._pos;
			const data = self._data;
			if (pos == data.len) {
				return null;
			}

			// TODO: for fixed length types, we don't need to decode the length
			const len_end = pos + 4;
			const len = std.mem.readIntBig(i32, data[pos..len_end][0..4]);

			const data_end = len_end + @as(usize, @intCast(len));
			std.debug.assert(data.len >= data_end);

			self._pos = data_end;
			return self._decoder(data[len_end..data_end]);
		}

		pub fn alloc(self: Self, allocator: Allocator) ![]T {
			var arr = try allocator.alloc(T, self.len);
			self.fill(arr);
			return arr;
		}

		pub fn fill(self: Self, arr: []T) void {
			const data = self._data;
			const decoder = self._decoder;

			var pos: usize = 0;
			for (0..self.len) |i| {
				// TODO: for fixed length types, we don't need to decode the length
				const len_end = pos + 4;
				const len = std.mem.readIntBig(i32, data[pos..len_end][0..4]);
				pos = len_end + @as(usize, @intCast(len));
				arr[i] = decoder(data[len_end..pos]);
			}
		}
	};
}

fn compileHaltGetError(comptime T: type) noreturn {
	@compileError("cannot get value of type " ++ @typeName(T));
}

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
		try t.expectEqual(32767, row.get(i16, 0));
		try t.expectEqual(2147483647, row.get(i32, 1));
		try t.expectEqual(9223372036854775807, row.get(i64, 2));

		try t.expectEqual(32767, row.get(?i16, 0));
		try t.expectEqual(2147483647, row.get(?i32, 1));
		try t.expectEqual(9223372036854775807, row.get(?i64, 2));

		try t.expectEqual(null, result.next());
	}

	{
		// int min
		var result = try c.query(sql, .{@as(i16, -32768), @as(i32, -2147483648), @as(i64, -9223372036854775808)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(-32768, row.get(i16, 0));
		try t.expectEqual(-2147483648, row.get(i32, 1));
		try t.expectEqual(-9223372036854775808, row.get(i64, 2));
		try result.drain();
	}

	{
		// int null
		var result = try c.query(sql, .{null, null, null});
		defer result.deinit();
		defer result.drain() catch unreachable;
		const row = (try result.next()).?;
		try t.expectEqual(null, row.get(?i16, 0));
		try t.expectEqual(null, row.get(?i32, 1));
		try t.expectEqual(null, row.get(?i64, 2));

	}

	{
		// uint within limit
		var result = try c.query(sql, .{@as(u16, 32767), @as(u32, 2147483647), @as(u64, 9223372036854775807)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(32767, row.get(i16, 0));
		try t.expectEqual(2147483647, row.get(i32, 1));
		try t.expectEqual(9223372036854775807, row.get(i64, 2));

		try t.expectEqual(32767, row.get(?i16, 0));
		try t.expectEqual(2147483647, row.get(?i32, 1));
		try t.expectEqual(9223372036854775807, row.get(?i64, 2));
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
		try t.expectEqual(1.23456, row.get(f32, 0));
		try t.expectEqual(1093.229183, row.get(f64, 1));

		try t.expectEqual(1.23456, row.get(?f32, 0));
		try t.expectEqual(1093.229183, row.get(?f64, 1));

		try t.expectEqual(null, result.next());
	}

	{
		// negative float
		var result = try c.query(sql, .{@as(f32, -392.31), @as(f64, -99991.99992)});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(-392.31, row.get(f32, 0));
		try t.expectEqual(-99991.99992, row.get(f64, 1));
		try t.expectEqual(null, result.next());
	}

	{
		// null float
		var result = try c.query(sql, .{null, null});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(null, row.get(?f32, 0));
		try t.expectEqual(null, row.get(?f64, 1));
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
		try t.expectEqual(true, row.get(bool, 0));
		try t.expectEqual(true, row.get(?bool, 0));
		try t.expectEqual(null, result.next());
	}

	{
		// false
		var result = try c.query(sql, .{false});
		defer result.deinit();
		defer result.drain() catch unreachable;
		const row = (try result.next()).?;
		try t.expectEqual(false, row.get(bool, 0));
		try t.expectEqual(false, row.get(?bool, 0));
		try t.expectEqual(null, result.next());
	}

	{
		// null
		var result = try c.query(sql, .{null});
		defer result.deinit();
		defer result.drain() catch unreachable;
		const row = (try result.next()).?;
		try t.expectEqual(null, row.get(?bool, 0));
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
		try t.expectString("", row.get([]u8, 0));
		try t.expectString("", row.get(?[]u8, 0).?);
		try t.expectString("", row.get([]u8, 1));
		try t.expectString("", row.get(?[]u8, 1).?);
		try result.drain();
	}

	{
		// not empty
		var result = try c.query(sql, .{"it's over 9000!!!", "i will Not fear"});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectString("it's over 9000!!!", row.get([]u8, 0));
		try t.expectString("it's over 9000!!!", row.get(?[]const u8, 0).?);
		try t.expectString("i will Not fear", row.get([]const u8, 1));
		try t.expectString("i will Not fear", row.get(?[]u8, 1).?);
		try result.drain();
	}

	{
		// as an array
		var result = try c.query(sql, .{[_]u8{'a', 'c', 'b'}, [_]u8{'z', 'z', '3'}});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectString("acb", row.get([]const u8, 0));
		try t.expectString("acb", row.get(?[]u8, 0).?);
		try t.expectString("zz3", row.get([]const u8, 1));
		try t.expectString("zz3", row.get(?[]u8, 1).?);
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
		try t.expectString("Leto", row.get([]u8, 0));
		try t.expectString("Leto", row.get(?[]u8, 0).?);
		try t.expectString("Ghanima", row.get([]u8, 1));
		try t.expectString("Ghanima", row.get(?[]u8, 1).?);
		try result.drain();
	}

	{
		// null
		var result = try c.query(sql, .{null, null});
		defer result.deinit();
		const row = (try result.next()).?;
		try t.expectEqual(null, row.get(?[]u8, 0));
		try t.expectEqual(null, row.get(?[]u8, 1));
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
		try t.expectEqual(321, row.get(i32, 0));

		try t.expectEqual(321, row.get(?i32, 0));
		try t.expectEqual(null, row.get(?i32, 1));
		try t.expectEqual(null, result.next());
	}
}

test "Result: iterator" {
	var c = t.connect(.{});
	defer c.deinit();

	{
		// empty
		var result = try c.query("select $1::int[]", .{[_]i32{}});
		defer result.deinit();
		var row = (try result.next()).?;

		var iterator = row.getIterator(i32, 0);
		try t.expectEqual(0, iterator.len);

		try t.expectEqual(null, iterator.next());
		try t.expectEqual(null, iterator.next());

		const a = try iterator.alloc(t.allocator);
		try t.expectEqual(0, a.len);
		try result.drain();
	}

	{
		// one
		var result = try c.query("select $1::int[]", .{[_]i32{9}});
		defer result.deinit();
		var row = (try result.next()).?;

		var iterator = row.getIterator(i32, 0);
		try t.expectEqual(1, iterator.len);

		try t.expectEqual(9, iterator.next());
		try t.expectEqual(null, iterator.next());

		const arr = try iterator.alloc(t.allocator);
		defer t.allocator.free(arr);
		try t.expectEqual(1, arr.len);
		try t.expectSlice(i32, &.{9}, arr);
		try result.drain();
	}

	{
		// fill
		var result = try c.query("select $1::int[]", .{[_]i32{0, -19}});
		defer result.deinit();
		var row = (try result.next()).?;

		var iterator = row.getIterator(i32, 0);
		try t.expectEqual(2, iterator.len);

		try t.expectEqual(0, iterator.next());
		try t.expectEqual(-19, iterator.next());
		try t.expectEqual(null, iterator.next());

		var arr: [2]i32 = undefined;
		iterator.fill(&arr);
		try t.expectSlice(i32, &.{0, -19}, &arr);
		try result.drain();
	}
}

test "Result: []int" {
	var c = t.connect(.{});
	defer c.deinit();
	const sql = "select $1::smallint[], $2::int[], $3::bigint[]";

	var result = try c.query(sql, .{[_]i16{-303, 9449, 2}, [_]i32{-3003, 49493229, 0}, [_]i64{944949338498392, -2}});
	defer result.deinit();

	var row = (try result.next()).?;

	const v1 = try row.getIterator(i16, 0).alloc(t.allocator);
	defer t.allocator.free(v1);
	try t.expectSlice(i16, &.{-303, 9449, 2}, v1);

	const v2 = try row.getIterator(i32, 1).alloc(t.allocator);
	defer t.allocator.free(v2);
	try t.expectSlice(i32, &.{-3003, 49493229, 0}, v2);

	const v3 = try row.getIterator(i64, 2).alloc(t.allocator);
	defer t.allocator.free(v3);
	try t.expectSlice(i64, &.{944949338498392, -2}, v3);
}

