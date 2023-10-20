const std = @import("std");
const buffer = @import("buffer");

const text_format = [_]u8{0, 0};
const binary_format = [_]u8{0, 1};

// These are nested inside the the Types structure so that we can generate an
// oid => encoding maping. See the oidEncoding function.
pub const Types = struct {

	pub const Int16 = struct {
		const oid = [_]u8{0, 0, 0, 21};
		const pg_send_format = &binary_format;

		fn encode(value: i16, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(&binary_format, format_pos);
			try buf.write(&.{0, 0, 0, 2}); // length of our data
			return buf.writeIntBig(i16, value);
		}

		fn encodeUnsigned(value: u16, buf: *buffer.Buffer, format_pos: usize) !void {
			if (value > 32767) return error.UnsignedIntWouldBeTruncated;
			return Int16.encode(@intCast(value), buf, format_pos);
		}

		pub fn decode(data: []const u8) i16 {
			std.debug.assert(data.len == 2);
			return std.mem.readIntBig(i16, data[0..2]);
		}
	};

	pub const Int32 = struct {
		const oid = [_]u8{0, 0, 0, 23};
		const pg_send_format = &binary_format;

		fn encode(value: i32, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(&binary_format, format_pos);
			try buf.write(&.{0, 0, 0, 4}); // length of our data
			return buf.writeIntBig(i32, value);
		}

		fn encodeUnsigned(value: u32, buf: *buffer.Buffer, format_pos: usize) !void {
			if (value > 2147483647) return error.UnsignedIntWouldBeTruncated;
			return Int32.encode(@intCast(value), buf, format_pos);
		}

		pub fn decode(data: []const u8) i32 {
			std.debug.assert(data.len == 4);
			return std.mem.readIntBig(i32, data[0..4]);
		}
	};

	pub const Int64 = struct {
		const oid = [_]u8{0, 0, 0, 20};
		const pg_send_format = &binary_format;

		fn encode(value: i64, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(&binary_format, format_pos);
			try buf.write(&.{0, 0, 0, 8}); // length of our data
			return buf.writeIntBig(i64, value);
		}

		fn encodeUnsigned(value: u64, buf: *buffer.Buffer, format_pos: usize) !void {
			if (value > 9223372036854775807) return error.UnsignedIntWouldBeTruncated;
			return Int64.encode(@intCast(value), buf, format_pos);
		}

		pub fn decode(data: []const u8) i64 {
			std.debug.assert(data.len == 8);
			return std.mem.readIntBig(i64, data[0..8]);
		}
	};

	pub const Float32 = struct {
		const oid = [_]u8{0, 0, 2, 188};
		const pg_send_format = &binary_format;

		fn encode(value: f32, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(&binary_format, format_pos);
			try buf.write(&.{0, 0, 0, 4}); // length of our data
			const t: *i32 = @constCast(@ptrCast(&value));
			return buf.writeIntBig(i32, t.*);
		}

		pub fn decode(data: []const u8) f32 {
			std.debug.assert(data.len == 4);
			const n = std.mem.readIntBig(i32, data[0..4]);
			const t : *f32 = @constCast(@ptrCast(&n));
			return t.*;
		}
	};

	pub const Float64 = struct {
		const oid = [_]u8{0, 0, 2, 189};
		const pg_send_format = &binary_format;

		fn encode(value: f64, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(&binary_format, format_pos);

			try buf.write(&.{0, 0, 0, 8}); // length of our data
			// not sure if this is the best option...
			const t: *i64 = @constCast(@ptrCast(&value));
			return buf.writeIntBig(i64, t.*);
		}

		pub fn decode(data: []const u8) f64 {
			std.debug.assert(data.len == 8);
			const n = std.mem.readIntBig(i64, data[0..8]);
			const t : *f64 = @constCast(@ptrCast(&n));
			return t.*;
		}
	};

	pub const Bool = struct {
		const oid = [_]u8{0, 0, 0, 16};
		const pg_send_format = &binary_format;

		fn encode(value: bool, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(&binary_format, format_pos);

			try buf.write(&.{0, 0, 0, 1}); // length of our data
			return buf.writeByte(if (value) 1 else 0);
		}

		pub fn decode(data: []const u8) bool {
			std.debug.assert(data.len == 1);
			return data[0] == 1;
		}
	};

	pub const String = struct {
		// https://www.postgresql.org/message-id/CAMovtNoHFod2jMAKQjjxv209PCTJx5Kc66anwWvX0mEiaXwgmA%40mail.gmail.com
		// says using the text format for text-like things is faster. There was
		// some other threads that discussed solutions, but it isn't clear if it was
		// ever fixed.
		const oid = [_]u8{0, 0, 0, 25};
		const pg_send_format = &text_format;

		fn encode(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(&text_format, format_pos);

			const space_needed = 4 + value.len;
			try buf.ensureUnusedCapacity(space_needed);
			var view = buf.view(buf.len());
			_ = try buf.skip(space_needed);

			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
		}

		pub fn decode(data: []const u8) []const u8 {
			return data;
		}
	};

	pub const Bytea = struct {
		const oid = [_]u8{0, 0, 0, 17};
		const pg_send_format = &binary_format;

		fn encode(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(&binary_format, format_pos);

			const space_needed = 4 + value.len;
			try buf.ensureUnusedCapacity(space_needed);
			var view = buf.view(buf.len());
			_ = try buf.skip(space_needed);

			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
		}
	};

	pub const Int16Array = struct {
		const oid = [_]u8{0, 0, 3, 237};
		const pg_send_format = &binary_format;

		fn encode(values: []const i16, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Int16.oid, oid_pos);
			return writeIntArray(i16, 2, values, buf);
		}

		fn encodeUnsigned(values: []const u16, buf: *buffer.Buffer, oid_pos: usize) !void {
			for (values) |v| {
				if (v > 32767) return error.UnsignedIntWouldBeTruncated;
			}
			buf.writeAt(&Int16.oid, oid_pos);
			return writeIntArray(i16, 2, values, buf);
		}
	};

	pub const Int32Array = struct {
		const oid = [_]u8{0, 0, 3, 239};
		const pg_send_format = &binary_format;

		fn encode(values: []const i32, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Int32.oid, oid_pos);
			return writeIntArray(i32, 4, values, buf);
		}

		fn encodeUnsigned(values: []const u32, buf: *buffer.Buffer, oid_pos: usize) !void {
			for (values) |v| {
				if (v > 2147483647) return error.UnsignedIntWouldBeTruncated;
			}
			buf.writeAt(&Int32.oid, oid_pos);
			return writeIntArray(i32, 4, values, buf);
		}

		pub fn decode(data: []const u8) i32 {
			std.debug.assert(data.len == 4);
			return std.mem.readIntBig(i32, data[0..4]);
		}
	};

	pub const Int64Array = struct {
		const oid = [_]u8{0, 0, 3, 248};
		const pg_send_format = &binary_format;

		fn encode(values: []const i64, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Int64.oid, oid_pos);
			return writeIntArray(i64, 8, values, buf);
		}

		fn encodeUnsigned(values: []const u64, buf: *buffer.Buffer, oid_pos: usize) !void {
			for (values) |v| {
				if (v > 9223372036854775807) return error.UnsignedIntWouldBeTruncated;
			}
			buf.writeAt(&Int64.oid, oid_pos);
			return writeIntArray(i64, 8, values, buf);
		}

		pub fn decode(data: []const u8) i64 {
			std.debug.assert(data.len == 8);
			return std.mem.readIntBig(i64, data[0..8]);
		}
	};

	// Return the encoding we want PG to use for a particular OID
	fn oidEncoding(oid: i32) *const [2]u8 {
		inline for (@typeInfo(@This()).Struct.decls) |decl| {
			const S = @field(@This(), decl.name);
			if (std.mem.readIntBig(i32, &S.oid) == oid) {
				return S.pg_send_format;
			}
		}
		return &text_format;
	}

	fn writeIntArray(comptime T: type, size: usize, values: []const T, buf: *buffer.Buffer) !void {
		const space_needed = (4 * values.len) + (values.len * size);
		try buf.ensureUnusedCapacity(space_needed);
		var view = buf.view(buf.len());
		_ = try buf.skip(space_needed);

		var value_len: [4]u8 = undefined;
		std.mem.writeIntBig(i32, &value_len, @intCast(size));
		for (values) |value| {
			view.write(&value_len);
			view.writeIntBig(T, value);
		}
	}
};

// expose our Types directly so callers can do types.Int32 rather than
// types.Types.Int32
pub usingnamespace Types;

// Writes 2 pieces of the Bind message: the parameter encoding types and
// the parameters themselves. Assumes buf is positioned correctly, i.e. the Bind
// message has been written up to but excluding
// "The number of parameter format codes that follow"
pub fn bindParameters(values: anytype, oids: []i32, buf: *buffer.Buffer) !void {
	if (values.len == 0) {
		// 0 as u16 Big (number of parameter types)
		// 0 as u16 Big (number of parameters)
		try buf.write(&.{0, 0, 0, 0});
		return;
	}

	// number of parameters types we're sending a
	try buf.writeIntBig(u16, @intCast(values.len));

	// for each parameter, we specify the format (text or binary), this is the
	// position within buf of where to write for the current parameter.
	var format_pos = buf.len();

	// every type takes 2 bytes (it's a u16 integer), pre-fill this with a text-type
	// for all parameters
	try buf.writeByteNTimes(0, values.len * 2);

	// number of parameters that we're sending
	try buf.writeIntBig(u16, @intCast(values.len));

	// buf looks something like/
	// 'B' - Bind Message
	//  0, 0, 0, 0 - Length Placeholder
	//  0, 3       - We're goint to send 3 param types
	//  0, 0       - Param Format 1 (we default to text)  <- format_pos
	//  0, 0       - Param Format 2 (we default to text)
	//  0, 0       - Param Format 3 (we default to text)
	//  0, 3       - We're going to send 3 param values
	//
	// At this point, we can use buf.write() to add values to the message
	// and we can use buf.writeAt(format_pos + (i*2)) to change the type

	inline for (values, oids) |value, oid| {
		try bindValue(@TypeOf(value), oid, value, buf, format_pos);
		format_pos += 2;
	}
}

// The oid is what PG is expecting. In some cases, we'll use that to figure
// out what to do.
fn bindValue(comptime T: type, oid: i32, value: anytype, buf: *buffer.Buffer, format_pos: usize) !void {
	switch (@typeInfo(T)) {
		.Null => {
			// type can stay 0 (text)
			// special length of -1 indicates null, no other data for this value
			return buf.write(&.{255, 255, 255, 255});
		},
		.ComptimeInt => {
			switch (oid) {
				21 => {
					if (value > 32767 or value < -32768) return error.IntWontFit;
					return Types.Int16.encode(@intCast(value), buf, format_pos);
				},
				23 => {
					if (value > 2147483647 or value < -2147483648) return error.IntWontFit;
					return Types.Int32.encode(@intCast(value), buf, format_pos);
				},
				else => return Types.Int64.encode(@intCast(value), buf, format_pos),
			}
		},
		.Int => {
			switch (oid) {
				21 => {
					if (value > 32767 or value < -32768) return error.IntWontFit;
					return Types.Int16.encode(@intCast(value), buf, format_pos);
				},
				23 => {
					if (value > 2147483647 or value < -2147483648) return error.IntWontFit;
					return Types.Int32.encode(@intCast(value), buf, format_pos);
				},
				else => {
					if (value > 9223372036854775807 or value < -9223372036854775808) return error.IntWontFit;
					return Types.Int64.encode(@intCast(value), buf, format_pos);
				},
			}
		},
		.ComptimeFloat => {
			switch (oid) {
				700 => return Types.Float32.encode(@floatCast(value), buf, format_pos),
				else => return Types.Float64.encode(@floatCast(value), buf, format_pos),
			}
		},
		.Float => |float| switch (float.bits) {
			32 => return Types.Float32.encode(@floatCast(value), buf, format_pos),
			64 => return Types.Float64.encode(@floatCast(value), buf, format_pos),
			else => compileHaltBindError(T),
		},
		.Bool => return Types.Bool.encode(value, buf, format_pos),
		.Pointer => |ptr| {
			switch (ptr.size) {
				.Slice => return bindSlice(ptr.child, oid, value, buf, format_pos),
				.One => {
					const E = std.meta.Elem(ptr.child);
					return bindSlice(E, oid, @as([]const E, value), buf, format_pos);
				},
				else => compileHaltBindError(T),
			}
		},
		.Array => return bindValue(@TypeOf(&value), oid, &value, buf, format_pos),
		.Optional => |opt| {
			if (value) |v| {
				return bindValue(opt.child, oid, v, buf, format_pos);
			}
			// null
			return buf.write(&.{255, 255, 255, 255});
		},
		else => {},
	}
}

fn bindSlice(comptime T: type, oid: i32, value: []const T, buf: *buffer.Buffer, format_pos: usize) !void {
	if (T == u8) {
		switch (oid) {
			17 => return Types.Bytea.encode(value, buf, format_pos),
			else => return Types.String.encode(value, buf, format_pos),
		}
	}

	const SliceT = []T;

	// We have an array. All arrays have the same header. We'll write this into
	// buf now. It's possible we don't support the array type, so this can still
	// fail.


	// arrays are always binary encoded (for now...)

	buf.writeAt(&binary_format, format_pos);

	const start_pos = buf.len();

	try buf.write(&.{
		0, 0, 0, 0, // placeholder for the lenght of this parameter
		0, 0, 0, 1, // number of dimensions, for now, we only support one
		0, 0, 0, 0, // bitmask of null, currently, with a single dimension, we don't have null arrays
		0, 0, 0, 0, // placeholder for the oid of each value
	});

	// where in buf, to write the OID of the values
	const oid_pos = buf.len() - 4;

	// number of values in our first (and currently only) dimension
	try buf.writeIntBig(i32, @intCast(value.len));
	try buf.write(&.{0, 0, 0, 1}); // lower bound of this demension

	switch (@typeInfo(T)) {
		.Int => |int| {
			if (int.signedness == .signed) {
				switch (int.bits) {
					16 => try Types.Int16Array.encode(value, buf, oid_pos),
					32 => try Types.Int32Array.encode(value, buf, oid_pos),
					64 => try Types.Int64Array.encode(value, buf, oid_pos),
					else => compileHaltBindError(SliceT),
				}
			} else {
				switch (int.bits) {
					1...16 => try Types.Int16Array.encodeUnsigned(value, buf, oid_pos),
					17...32 => try Types.Int32Array.encodeUnsigned(value, buf, oid_pos),
					33...64 => try Types.Int64Array.encodeUnsigned(value, buf, oid_pos),
					else => compileHaltBindError(SliceT),
				}
			}
		},
		else => compileHaltBindError(SliceT),
	}

	var param_len: [4]u8 = undefined;
	// write the lenght of the parameter, -4 because for paremeters, the length
	// prefix itself isn't included.
	std.mem.writeIntBig(i32, &param_len, @intCast(buf.len() - start_pos - 4));
	buf.writeAt(&param_len, start_pos);
}

// Write the last part of the Bind message: telling postgresql how it should
// encode each column of the response
pub fn resultEncoding(oids: []i32, buf: *buffer.Buffer) !void {
	if (oids.len == 0) {
		return buf.write(&.{0, 0}); // we are specifying 0 return types
	}

	// 2 bytes for the # of columns we're specifying + 2 bytes per column
	const space_needed = 2 + oids.len * 2;
	try buf.ensureUnusedCapacity(space_needed);
	var view = buf.view(buf.len());
	_ = try buf.skip(space_needed);

	view.writeIntBig(u16, @intCast(oids.len));
	for (oids) |oid| {
		view.write(Types.oidEncoding(oid));
	}
}

fn compileHaltBindError(comptime T: type) noreturn {
	@compileError("cannot bind value of type " ++ @typeName(T));
}
