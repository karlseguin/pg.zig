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

		fn encode(value: i16, buf: *buffer.Buffer, type_pos: usize) !void {
			buf.writeAt(&binary_format, type_pos);
			try buf.write(&.{0, 0, 0, 2}); // length of our data
			return buf.writeIntBig(i16, value);
		}

		pub fn decode(data: []const u8) i16 {
			std.debug.assert(data.len == 2);
			return std.mem.readIntBig(i16, data[0..2]);
		}
	};

	pub const Int32 = struct {
		const oid = [_]u8{0, 0, 0, 23};
		const pg_send_format = &binary_format;

		fn encode(value: i32, buf: *buffer.Buffer, type_pos: usize) !void {
			buf.writeAt(&binary_format, type_pos);
			try buf.write(&.{0, 0, 0, 4}); // length of our data
			return buf.writeIntBig(i32, value);
		}

		pub fn decode(data: []const u8) i32 {
			std.debug.assert(data.len == 4);
			return std.mem.readIntBig(i32, data[0..4]);
		}
	};

	pub const Int64 = struct {
		const oid = [_]u8{0, 0, 0, 20};
		const pg_send_format = &binary_format;

		fn encode(value: i64, buf: *buffer.Buffer, type_pos: usize) !void {
			buf.writeAt(&binary_format, type_pos);
			try buf.write(&.{0, 0, 0, 8}); // length of our data
			return buf.writeIntBig(i64, value);
		}

		pub fn decode(data: []const u8) i64 {
			std.debug.assert(data.len == 8);
			return std.mem.readIntBig(i64, data[0..8]);
		}
	};

	pub const Float32 = struct {
		const oid = [_]u8{0, 0, 2, 188};
		const pg_send_format = &binary_format;

		fn encode(value: f32, buf: *buffer.Buffer, type_pos: usize) !void {
			buf.writeAt(&binary_format, type_pos);
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

		fn encode(value: f64, buf: *buffer.Buffer, type_pos: usize) !void {
			buf.writeAt(&binary_format, type_pos);

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

		fn encode(value: bool, buf: *buffer.Buffer, type_pos: usize) !void {
			buf.writeAt(&binary_format, type_pos);

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

		fn encode(value: []const u8, buf: *buffer.Buffer, type_pos: usize) !void {
			buf.writeAt(&text_format, type_pos);

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

		fn encode(value: []const u8, buf: *buffer.Buffer, type_pos: usize) !void {
			buf.writeAt(&binary_format, type_pos);

			const space_needed = 4 + value.len;
			try buf.ensureUnusedCapacity(space_needed);
			var view = buf.view(buf.len());
			_ = try buf.skip(space_needed);

			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
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

	// where to write our next type
	var type_pos = buf.len();

	// every type takes 2 bytes (it's a u16 integer), pre-fill this with a text-type
	// for all parameters
	try buf.writeByteNTimes(0, values.len * 2);

	// number of parameters that we're sending
	try buf.writeIntBig(u16, @intCast(values.len));

	// buf looks something like/
	// 'B' - Bind Message
	//  0, 0, 0, 0 - Length Placeholder
	//  0, 3       - We're goint to send 3 param types
	//  0, 0       - Param Type 1 (we default to text)  <- type_pos
	//  0, 0       - Param Type 2 (we default to text)
	//  0, 0       - Param Type 3 (we default to text)
	//  0, 3       - We're going to send 3 param values
	//
	// At this point, we can use buf.write() to add values to the message
	// and we can use buf.writeAt(type_pos + (i*2)) to change the type

	inline for (values, oids) |value, oid| {
		try bindValue(@TypeOf(value), oid, value, buf, type_pos);
		type_pos += 2;
	}
}

// The oid is what PG is expecting. In some cases, we'll use that to figure
// out what to do.
fn bindValue(comptime T: type, oid: i32, value: anytype, buf: *buffer.Buffer, type_pos: usize) !void {
	switch (@typeInfo(T)) {
		.Null => {
			// type can stay 0 (text)
			// special length of -1 indicates null, no other data for this value
			return buf.write(&.{255, 255, 255, 255});
		},
		.ComptimeInt => {
			switch (oid) {
				21 => {
					if (value > 32767 or value < -32768) return error.ComptimeIntWouldBeTruncated;
					return Types.Int16.encode(@intCast(value), buf, type_pos);
				},
				23 => {
					if (value > 2147483647 or value < -2147483648) return error.ComptimeIntWouldBeTruncated;
					return Types.Int32.encode(@intCast(value), buf, type_pos);
				},
				else => return Types.Int64.encode(@intCast(value), buf, type_pos),
			}
		},
		.Int => |int| {
			if (int.signedness == .signed) {
				switch (int.bits) {
					1...16 => return Types.Int16.encode(@intCast(value), buf, type_pos),
					17...32 => return Types.Int32.encode(@intCast(value), buf, type_pos),
					33...64 => return Types.Int64.encode(@intCast(value), buf, type_pos),
					else => compileHaltBindError(T),
				}
			} else {
				// All ints in PG are signed. This library accepts u16/u32/u64, but it
				// will make sure the value is fits in a i16/i32/i64
				switch (int.bits) {
					16 => if (value > 32767) return error.UnsignedIntWouldBeTruncated,
					32 => if (value > 2147483647) return error.UnsignedIntWouldBeTruncated,
					64 => if (value > 9223372036854775807) return error.UnsignedIntWouldBeTruncated,
					else => {}
				}
				// If we're dealing iwth a u16/u32/u64, then the above switch checked
				// that the value can safely be cast to an i16/i32/i64.
				switch (int.bits) {
					1...16 => return Types.Int16.encode(@intCast(value), buf, type_pos),
					17...32 => return Types.Int32.encode(@intCast(value), buf, type_pos),
					33...64 => return Types.Int64.encode(@intCast(value), buf, type_pos),
					else => compileHaltBindError(T),
				}
			}
		},
		.ComptimeFloat => {
			switch (oid) {
				700 => return Types.Float32.encode(@floatCast(value), buf, type_pos),
				else => return Types.Float64.encode(@floatCast(value), buf, type_pos),
			}
		},
		.Float => |float| switch (float.bits) {
			1...32 => return Types.Float32.encode(@floatCast(value), buf, type_pos),
			33...64 => return Types.Float64.encode(@floatCast(value), buf, type_pos),
			else => compileHaltBindError(T),
		},
		.Bool => return Types.Bool.encode(value, buf, type_pos),
		.Pointer => |ptr| {
			switch (ptr.size) {
				.One => return bindValue(ptr.child, oid, value, buf, type_pos),
				.Slice => switch (ptr.child) {
					u8 => switch (oid) {
						17 => return Types.Bytea.encode(value, buf, type_pos),
						else => return Types.String.encode(value, buf, type_pos),
					},
					else => compileHaltBindError(T),
				},
				else => compileHaltBindError(T),
			}
		},
		.Array => |arr| switch (arr.child) {
			u8 => switch (oid) {
				17 => return Types.Bytea.encode(value[0..], buf, type_pos),
				else => return Types.String.encode(value[0..], buf, type_pos),
			},
			else => compileHaltBindError(T),
		},
		.Optional => |opt| {
			if (value) |v| {
				return bindValue(opt.child, oid, v, buf, type_pos);
			}
			// null
			return buf.write(&.{255, 255, 255, 255});
		},
		else => {},
	}
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
