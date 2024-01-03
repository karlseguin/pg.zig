const std = @import("std");
const lib = @import("lib.zig");
const buffer = @import("buffer");

// These are nested inside the the Types structure so that we can generate an
// oid => encoding maping. See the oidEncoding function.
pub const OID = struct {
	decimal: i32,
	encoded: [4]u8,

	pub fn make(decimal: i32) OID {
		var encoded: [4]u8 = undefined;
		std.mem.writeInt(i32, &encoded, decimal, .big);
		return .{
			.decimal = decimal,
			.encoded = encoded,
		};
	}
};

pub const text_encoding = [2]u8{0, 0};
pub const binary_encoding = [2]u8{0, 1};

// Any "decodeKnown" you see is just an optimization to avoid extra assertions
// when decoding an individual array value. Once we know the array type, we don't
// need to assert the oid of each individual value.

pub const Types = struct {
	// Every supported type is here. This includes the format we want to
	// encode/decode (text or binary), and the logic for encoding and decoding.

	pub usingnamespace @import("types/cidr.zig");
	pub usingnamespace @import("types/numeric.zig");

	pub const Char = struct {
		// A blank-padded char
		pub const oid = OID.make(1042);
		const encoding = &binary_encoding;

		fn encode(value: u8, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Char.encoding, format_pos);
			try buf.write(&.{0, 0, 0, 1}); // length of our data
			return buf.writeByte(value);
		}

		pub fn decode(data: []const u8, data_oid: i32) u8 {
			lib.assert(data_oid == Char.oid.decimal);
			return data[0];
		}

		pub fn decodeKnown(data: []const u8) u8 {
			return data[0];
		}
	};

	pub const Int16 = struct {
		pub const oid = OID.make(21);
		const encoding = &binary_encoding;

		fn encode(value: i16, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Int16.encoding, format_pos);
			try buf.write(&.{0, 0, 0, 2}); // length of our data
			return buf.writeIntBig(i16, value);
		}

		fn encodeUnsigned(value: u16, buf: *buffer.Buffer, format_pos: usize) !void {
			if (value > 32767) return error.UnsignedIntWouldBeTruncated;
			return Int16.encode(@intCast(value), buf, format_pos);
		}

		pub fn decode(data: []const u8, data_oid: i32) i16 {
			lib.assert(data_oid == Int16.oid.decimal);
			return Int16.decodeKnown(data);
		}

		pub fn decodeKnown(data: []const u8) i16 {
			return std.mem.readInt(i16, data[0..2], .big);
		}
	};

	pub const Int32 = struct {
		pub const oid = OID.make(23);
		const encoding = &binary_encoding;

		fn encode(value: i32, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Int32.encoding, format_pos);
			try buf.write(&.{0, 0, 0, 4}); // length of our data
			return buf.writeIntBig(i32, value);
		}

		fn encodeUnsigned(value: u32, buf: *buffer.Buffer, format_pos: usize) !void {
			if (value > 2147483647) return error.UnsignedIntWouldBeTruncated;
			return Int32.encode(@intCast(value), buf, format_pos);
		}

		pub fn decode(data: []const u8, data_oid: i32) i32 {
			lib.assert(data_oid == Int32.oid.decimal);
			return Int32.decodeKnown(data);
		}

		pub fn decodeKnown(data: []const u8) i32 {
			return std.mem.readInt(i32, data[0..4], .big);
		}
	};

	pub const Int64 = struct {
		pub const oid = OID.make(20);
		const encoding = &binary_encoding;

		fn encode(value: i64, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Int64.encoding, format_pos);
			try buf.write(&.{0, 0, 0, 8}); // length of our data
			return buf.writeIntBig(i64, value);
		}

		fn encodeUnsigned(value: u64, buf: *buffer.Buffer, format_pos: usize) !void {
			if (value > 9223372036854775807) return error.UnsignedIntWouldBeTruncated;
			return Int64.encode(@intCast(value), buf, format_pos);
		}

		pub fn decode(data: []const u8, data_oid: i32) i64 {
			switch (data_oid) {
				Timestamp.oid.decimal, TimestampTz.oid.decimal => return Timestamp.decode(data, data_oid),
				else => {
					lib.assert(data_oid == Int64.oid.decimal);
					return Int64.decodeKnown(data);
				},
			}
		}

		pub fn decodeKnown(data: []const u8) i64 {
			return std.mem.readInt(i64, data[0..8], .big);
		}
	};

	pub const Timestamp = struct {
		pub const oid = OID.make(1114);
		const encoding = &binary_encoding;
		const us_from_epoch_to_y2k = 946_684_800_000_000;

		fn encode(value: i64, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Timestamp.encoding, format_pos);
			try buf.write(&.{0, 0, 0, 8}); // length of our data
			return buf.writeIntBig(i64, value - us_from_epoch_to_y2k);
		}

		pub fn decode(data: []const u8, data_oid: i32) i64 {
			lib.assert(data_oid == Timestamp.oid.decimal or data_oid == TimestampTz.oid.decimal);
			return std.mem.readInt(i64, data[0..8], .big) + us_from_epoch_to_y2k;
		}

		pub fn decodeKnown(data: []const u8) i64 {
			return std.mem.readInt(i64, data[0..8], .big) + us_from_epoch_to_y2k;
		}
	};

	pub const TimestampTz = struct {
		pub const oid = OID.make(1184);
		const encoding = &binary_encoding;
	};

	pub const Float32 = struct {
		pub const oid = OID.make(700);
		const encoding = &binary_encoding;

		fn encode(value: f32, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Float32.encoding, format_pos);
			try buf.write(&.{0, 0, 0, 4}); // length of our data
			const tmp: *i32 = @constCast(@ptrCast(&value));
			return buf.writeIntBig(i32, tmp.*);
		}

		pub fn decode(data: []const u8, data_oid: i32) f32 {
			lib.assert(data_oid == Float32.oid.decimal);
			return Float32.decodeKnown(data);
		}

		pub fn decodeKnown(data: []const u8) f32 {
			const n = std.mem.readInt(i32, data[0..4], .big);
			const tmp: *f32 = @constCast(@ptrCast(&n));
			return tmp.*;
		}
	};

	pub const Float64 = struct {
		pub const oid = OID.make(701);
		const encoding = &binary_encoding;

		fn encode(value: f64, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Float64.encoding, format_pos);

			try buf.write(&.{0, 0, 0, 8}); // length of our data
			// not sure if this is the best option...
			const tmp: *i64 = @constCast(@ptrCast(&value));
			return buf.writeIntBig(i64, tmp.*);
		}

		pub fn decode(data: []const u8, data_oid: i32) f64 {
			switch (data_oid) {
				Types.Numeric.oid.decimal => return Types.Numeric.decode(data, data_oid).toFloat(),
				else => {
					lib.assert(data_oid == Float64.oid.decimal);
					return Float64.decodeKnown(data);
				},
			}
		}

		pub fn decodeKnown(data: []const u8) f64 {
			const n = std.mem.readInt(i64, data[0..8], .big);
			const tmp: *f64 = @constCast(@ptrCast(&n));
			return tmp.*;
		}
	};

	pub const Bool = struct {
		pub const oid = OID.make(16);
		const encoding = &binary_encoding;

		fn encode(value: bool, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Bool.encoding, format_pos);
			try buf.write(&.{0, 0, 0, 1}); // length of our data
			return buf.writeByte(if (value) 1 else 0);
		}

		pub fn decode(data: []const u8, data_oid: i32) bool {
			lib.assert(data_oid == Bool.oid.decimal);
			return decodeKnown(data);
		}

		pub fn decodeKnown(data: []const u8) bool {
			return data[0] == 1;
		}
	};

	pub const String = struct {
		pub const oid = OID.make(25);
		// https://www.postgresql.org/message-id/CAMovtNoHFod2jMAKQjjxv209PCTJx5Kc66anwWvX0mEiaXwgmA%40mail.gmail.com
		// says using the text format for text-like things is faster. There was
		// some other threads that discussed solutions, but it isn't clear if it was
		// ever fixed.
		const encoding = &text_encoding;

		fn encode(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(String.encoding, format_pos);
			var view = try buf.skip(4 + value.len);
			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
		}
	};

	pub const Bytea = struct {
		pub const oid = OID.make(17);
		const encoding = &binary_encoding;

		fn encode(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(Bytea.encoding, format_pos);
			var view = try buf.skip(4 + value.len);
			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
		}

		pub fn decode(data: []const u8, data_oid: i32) []const u8 {
			switch (data_oid) {
				JSONB.oid.decimal => return JSONB.decodeKnown(data),
				else => return data,
			}
		}

		pub fn decodeKnown(data: []const u8) []const u8 {
			return data;
		}

		pub fn decodeKnownMutable(data: []const u8) []u8 {
			// we know the underlying []u8 is mutable, it comes from our Reader
			return @constCast(data);
		}
	};

	pub const UUID = struct {
		pub const oid = OID.make(2950);
		const encoding = &binary_encoding;

		fn encode(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(UUID.encoding, format_pos);
			var view = try buf.skip( 20);
			view.write(&.{0, 0, 0, 16});
			switch (value.len) {
				16 => view.write(value),
				36 => view.write(&(try UUID.toBytes(value))),
				else => return error.InvalidUUID,
			}
		}

		pub fn decode(data: []const u8, data_oid: i32) []const u8 {
			lib.assert(data_oid == UUID.oid.decimal);
			return data;
		}

		const hex = "0123456789abcdef";
		const encoded_pos = [16]u8{ 0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34 };
		const hex_to_nibble = [256]u8{
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x08, 0x09, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		};

		pub fn toString(uuid: []const u8) ![36]u8 {
			if (uuid.len != 16) {
				return error.InvalidUUID;
			}

			var out: [36]u8 = undefined;
			out[8] = '-';
			out[13] = '-';
			out[18] = '-';
			out[23] = '-';

			inline for (encoded_pos, 0..) |i, j| {
				out[i + 0] = hex[uuid[j] >> 4];
				out[i + 1] = hex[uuid[j] & 0x0f];
			}
			return out;
		}

		pub fn toBytes(str: []const u8) ![16]u8 {
			if (str.len != 36 or str[8] != '-' or str[13] != '-' or str[18] != '-' or str[23] != '-') {
				return error.InvalidUUID;
			}

			var out: [16]u8 = undefined;
			inline for (encoded_pos, 0..) |i, j| {
				const hi = hex_to_nibble[str[i + 0]];
				const lo = hex_to_nibble[str[i + 1]];
				if (hi == 0xff or lo == 0xff) {
					return error.InvalidUUID;
				}
				out[j] = hi << 4 | lo;
			}
			return out;
		}
	};

	pub const MacAddr = struct {
		pub const oid = OID.make(829);
		const encoding = &binary_encoding;

		fn encode(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			if (value.len != 6) {
				// assume this is a text representation
				return String.encode(value, buf, format_pos);
			}
			buf.writeAt(MacAddr.encoding, format_pos);
			var view = try buf.skip(4 + value.len);
			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
		}
	};

	pub const MacAddr8 = struct {
		pub const oid = OID.make(774);
		const encoding = &binary_encoding;

		fn encode(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			if (value.len != 8) {
				// assume this is a text representation
				return String.encode(value, buf, format_pos);
			}
			buf.writeAt(MacAddr8.encoding, format_pos);
			var view = try buf.skip(4 + value.len);
			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
		}
	};

	pub const JSON = struct {
		pub const oid = OID.make(114);
		const encoding = &binary_encoding;

		fn encodeBytes(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(JSON.encoding, format_pos);
			var view = try buf.skip(4 + value.len);
			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
		}

		fn encode(value: anytype, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(JSON.encoding, format_pos);
			const state = try Encode.variableLengthStart(buf);
			try std.json.stringify(value, .{}, buf.writer());
			Encode.variableLengthFill(buf, state);
		}
	};

	pub const JSONB = struct {
		pub const oid = OID.make(3802);
		const encoding = &binary_encoding;

		fn encodeBytes(value: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(JSONB.encoding, format_pos);
			var view = try buf.skip( 5 + value.len);
			// + 1 for the version
			view.writeIntBig(i32, @intCast(value.len + 1));
			view.writeByte(1); // jsonb version
			view.write(value);
		}

		fn encode(value: anytype, buf: *buffer.Buffer, format_pos: usize) !void {
			buf.writeAt(JSON.encoding, format_pos);
			const state = try Encode.variableLengthStart(buf);
			try buf.writeByte(1); // jsonb version
			try std.json.stringify(value, .{}, buf.writer());
			Encode.variableLengthFill(buf, state);
		}

		fn decode(data: []const u8, data_oid: i32) []const u8 {
			lib.assert(data_oid == JSONB.oid.decimal);
			return JSONB.decodeKnown(data);
		}

		pub fn decodeKnown(data: []const u8) []const u8 {
			return data[1..];
		}

		pub fn decodeKnownMutable(data: []const u8) []u8 {
			// we know the underlying []u8 is mutable, it comes from our Reader
			return @constCast(data[1..]);
		}
	};

	pub const Int16Array = struct {
		pub const oid = OID.make(1005);
		const encoding = &binary_encoding;

		fn encode(values: []const i16, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Int16.oid.encoded, oid_pos);
			return Encode.writeIntArray(i16, 2, values, buf);
		}

		fn encodeUnsigned(values: []const u16, buf: *buffer.Buffer, oid_pos: usize) !void {
			for (values) |v| {
				if (v > 32767) return error.UnsignedIntWouldBeTruncated;
			}
			buf.writeAt(&Int16.oid, oid_pos);
			return Encode.writeIntArray(i16, 2, values, buf);
		}
	};

	pub const Int32Array = struct {
		pub const oid = OID.make(1007);
		const encoding = &binary_encoding;

		fn encode(values: []const i32, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Int32.oid.encoded, oid_pos);
			return Encode.writeIntArray(i32, 4, values, buf);
		}

		fn encodeUnsigned(values: []const u32, buf: *buffer.Buffer, oid_pos: usize) !void {
			for (values) |v| {
				if (v > 2147483647) return error.UnsignedIntWouldBeTruncated;
			}
			buf.writeAt(&Int32.oid, oid_pos);
			return Encode.writeIntArray(i32, 4, values, buf);
		}
	};

	pub const Int64Array = struct {
		pub const oid = OID.make(1016);
		const encoding = &binary_encoding;

		fn encode(values: []const i64, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Int64.oid.encoded, oid_pos);
			return Encode.writeIntArray(i64, 8, values, buf);
		}

		fn encodeUnsigned(values: []const u64, buf: *buffer.Buffer, oid_pos: usize) !void {
			for (values) |v| {
				if (v > 9223372036854775807) return error.UnsignedIntWouldBeTruncated;
			}
			buf.writeAt(&Int64.oid, oid_pos);
			return Encode.writeIntArray(i64, 8, values, buf);
		}
	};

	pub const TimestampArray = struct {
		pub const oid = OID.make(1115);
		const encoding = &binary_encoding;
		const us_from_epoch_to_y2k = 946_684_800_000_000;

		fn encode(values: []const i64, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Timestamp.oid.encoded, oid_pos);

			// every value is 12 bytes, 4 byte length + 8 byte value
			var view = try buf.skip(12 * values.len);
			for (values) |value| {
				view.write(&.{0, 0, 0, 8}); // length of value
				view.writeIntBig(i64, value - us_from_epoch_to_y2k);
			}
		}
	};

	pub const TimestampTzArray = struct {
		pub const oid = OID.make(1185);
		const encoding = &binary_encoding;

		const us_from_epoch_to_y2k = 946_684_800_000_000;

		fn encode(values: []const i64, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&TimestampTz.oid.encoded, oid_pos);

			// every value is 12 bytes, 4 byte length + 8 byte value
			var view = try buf.skip(12 * values.len);
			for (values) |value| {
				view.write(&.{0, 0, 0, 8}); // length of value
				view.writeIntBig(i64, value - us_from_epoch_to_y2k);
			}
		}
	};

	pub const Float32Array = struct {
		pub const oid = OID.make(1021);
		const encoding = &binary_encoding;

		fn encode(values: []const f32, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Float32.oid.encoded, oid_pos);

			// every value takes 8 bytes, 4 for the length, 4 for the value
			var view = try buf.skip(8 * values.len);
			for (values) |value| {
				view.write(&.{0, 0, 0, 4}); //length
				const tmp: *i32 = @constCast(@ptrCast(&value));
				view.writeIntBig(i32, tmp.*);
			}
		}
	};

	pub const Float64Array = struct {
		pub const oid = OID.make(1022);
		const encoding = &binary_encoding;

		fn encode(values: []const f64, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Float64.oid.encoded, oid_pos);

			// every value takes 12 bytes, 4 for the length, 8 for the value
			var view = try buf.skip(12 * values.len);
			for (values) |value| {
				view.write(&.{0, 0, 0, 8}); //length
				const tmp: *i64 = @constCast(@ptrCast(&value));
				view.writeIntBig(i64, tmp.*);
			}
		}
	};

	pub const BoolArray = struct {
		pub const oid = OID.make(1000);
		const encoding = &binary_encoding;

		fn encode(values: []const bool, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Bool.oid.encoded, oid_pos);

			// every value takes 5 bytes, 4 for the length, 1 for the value
			var view = try buf.skip(5 * values.len);
			for (values) |value| {
				// each value is prefixed with a 4 byte length
				if (value) {
					view.write(&.{0, 0, 0, 1, 1});
				} else {
					view.write(&.{0, 0, 0, 1, 0});
				}
			}
		}
	};

	pub const NumericArray = struct {
		pub const oid = OID.make(1231);
		const encoding = &binary_encoding;
		fn encode(values: anytype, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Types.Numeric.oid.encoded, oid_pos);

			for (values) |value| {
				try Types.Numeric.encodeBuf(value, buf);
			}
		}
	};

	pub const CidrArray = struct {
		pub const oid = OID.make(651);
		pub const inet_oid = OID.make(1041);
		const encoding = &binary_encoding;
	};

	pub const MacAddrArray = struct {
		pub const oid = OID.make(1040);
		const encoding = &binary_encoding;

		fn encode(values: []const []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			// This has challenges. Do we have a binary representation or a text representation?
			// Or maybe we have a mix (maybe we shouldn't support that)?
			// We handle this with UUID by converting the text representation to binary
			// but it's harder wit MacAddr because it supports 7 different text representations
			// and I don't really want this library to become a text parsing library which attempts
			// to mimic what PostgreSQL does.
			// So we're going to send a text-encoded array with text values, which emans
			// we need to convert any binary representation to text (which is a lot easier).

			// The worst-case scenario is that each value takes 17 bytes. This is the
			// most verbose text-encoded value. When we encode a binary value as text
			// we'll use the most compact (12 bytes), but we might be given a 17-byte
			// text-encoded value, which we'll write as-is
			var l: usize = 0;
			for (values) |v| {
				// binary values will be encoded in a 12-byte text representation
				l += if (v.len == 6) 12 else v.len;
			}

			return Encode.writeTextEncodedArray(values, l, buf, format_pos, MacAddrArray.writeOneAsText);
		}

		fn writeOneAsText(value: []const u8, buf: *buffer.Buffer) void {
			if (value.len == 6) {
				std.fmt.format(buf.writer(), "{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{value[0], value[1], value[2], value[3], value[4], value[5]}) catch unreachable;
			} else {
				buf.writeAssumeCapacity(value);
			}
		}
	};

	pub const MacAddr8Array = struct {
		pub const oid = OID.make(775);
		const encoding = &binary_encoding;

		fn encode(values: []const []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
			// See comments in MacAddrArray.encode
			var l: usize = 0;
			for (values) |v| {
				// binary values will be encoded in a 16-byte text representation
				l += if (v.len == 8) 16 else v.len;
			}

			return Encode.writeTextEncodedArray(values, l, buf, format_pos, MacAddr8Array.writeOneAsText);
		}

		fn writeOneAsText(value: []const u8, buf: *buffer.Buffer) void {
			if (value.len == 8) {
				std.fmt.format(buf.writer(), "{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7]}) catch unreachable;
			} else {
				buf.writeAssumeCapacity(value);
			}
		}
	};

	pub const ByteaArray = struct {
		pub const oid = OID.make(1001);
		const encoding = &binary_encoding;

		fn encode(values: []const []const u8, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Bytea.oid.encoded, oid_pos);
			return Encode.writeByteArray(values, buf);
		}
	};

	pub const StringArray = struct {
		pub const oid = OID.make(1009);
		const encoding = &binary_encoding;

		fn encode(values: []const []const u8, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&String.oid.encoded, oid_pos);
			return Encode.writeByteArray(values, buf);
		}

		fn encodeEnum(values: anytype, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&String.oid.encoded, oid_pos);
			for (values.*) |value| {
				const str = @tagName(value);
				try buf.writeIntBig(i32, @intCast(str.len));
				try buf.write(str);
			}
		}
	};

	pub const UUIDArray = struct {
		pub const oid = OID.make(2951);
		const encoding = &binary_encoding;

		fn encode(values: []const []const u8, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&UUID.oid.encoded, oid_pos);

			// every value is 20 bytes, 4 byte length + 16 byte value
			var view = try buf.skip(20 * values.len);
			for (values) |value| {
				view.write(&.{0, 0, 0, 16}); // length of value
				switch (value.len) {
					16 => view.write(value),
					36 => view.write(&(try UUID.toBytes(value))),
					else => return error.InvalidUUID,
				}
			}
		}
	};

	pub const JSONArray = struct {
		pub const oid = OID.make(199);
		const encoding = &binary_encoding;

		fn encode(values: []const []const u8, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&JSON.oid.encoded, oid_pos);
			return Encode.writeByteArray(values, buf);
		}
	};

	pub const JSONBArray = struct {
		pub const oid = OID.make(3807);
		const encoding = &binary_encoding;

		fn encode(values: []const []const u8, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&JSONB.oid.encoded, oid_pos);

			// every value has a 5 byte prefix, a 4 byte length and a 1 byte version
			var len = values.len * 5;
			for (values) |value| {
				len += value.len;
			}

			var view = try buf.skip(len);
			for (values) |value| {
				// + 1 for the version
				view.writeIntBig(i32, @intCast(value.len + 1));
				view.writeByte(1); // version
				view.write(value);
			}
		}
	};

	pub const CharArray = struct {
		pub const oid = OID.make(1014);
		const encoding = &binary_encoding;

		// This is for a char[] bound to a []u8
		fn encodeOne(values: []const u8, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Char.oid.encoded, oid_pos);

			// every value has a 5 byte prefix, a 4 byte length and a 1 byte char
			const len = values.len * 5;
			var view = try buf.skip(len);
			for (values) |value| {
				view.write(&.{0, 0, 0, 1});
				view.writeByte(value);
			}
		}

		// This is for a char[] bound to a [][]u8
		fn encode(values: []const []const u8, buf: *buffer.Buffer, oid_pos: usize) !void {
			buf.writeAt(&Char.oid.encoded, oid_pos);
			return Encode.writeByteArray(values, buf);
		}
	};

	// Return the encoding we want PG to use for a particular OID
	fn resultEncodingFor(oid: i32) *const [2]u8 {
		inline for (@typeInfo(@This()).Struct.decls) |decl| {
			const S = @field(@This(), decl.name);
			if (oid == S.oid.decimal) {
				return S.encoding;
			}
		}
		// default to text encoding
		return &binary_encoding;
	}
};

// expose our Types directly so callers can do types.Int32 rather than
// types.Types.Int32
pub usingnamespace Types;

pub const Encode = struct {
	// helpers for encoding data (or part of the data)
	pub fn writeIntArray(comptime T: type, size: usize, values: []const T, buf: *buffer.Buffer) !void {
		var view = try buf.skip((size + 4) * values.len);

		var value_len: [4]u8 = undefined;
		std.mem.writeInt(i32, &value_len, @intCast(size), .big);
		for (values) |value| {
			view.write(&value_len);
			view.writeIntBig(T, value);
		}
	}

	pub fn writeByteArray(values: []const []const u8, buf: *buffer.Buffer) !void {
		// each value has a 4 byte length prefix
		var len = values.len * 4;
		for (values) |value| {
			len += value.len;
		}

		var view = try buf.skip(len);
		for (values) |value| {
			view.writeIntBig(i32, @intCast(value.len));
			view.write(value);
		}
	}

	pub fn variableLengthStart(buf: *buffer.Buffer) !usize {
		try buf.write(&.{0, 0, 0, 0}); // length placeholder
		return buf.len();
	}

	pub fn variableLengthFill(buf: *buffer.Buffer, pos: usize) void {
		const len = buf.len() - pos;
		var encoded_len: [4]u8 = undefined;
		std.mem.writeInt(i32, &encoded_len, @intCast(len), .big);
		buf.writeAt(&encoded_len, pos - 4);
	}

	pub fn writeTextEncodedArray(values: []const []const u8, values_len: usize, buf: *buffer.Buffer, format_pos: usize, writeFn: *const fn([]const u8, *buffer.Buffer) void) !void {
		buf.writeAt(&text_encoding, format_pos);
		if (values.len == 0) {
			// empty array, with length prefix
			return buf.write(&.{0, 0, 0, 2, '{', '}'});
		}

		// We're relying one our caller to give us an accurate values_len
		// The total value length will be:
		//  2             + values_len    + values.len
		//  {} delimiter  + given to us   + ',' delimiter between values
		const max_len = 2 + values_len + values.len;
		try buf.ensureUnusedCapacity(max_len);

		// our max_len is just an estimate, we'll get the actual length and fill
		// it in later, for now, we skip the length
		var view = try buf.skip(4);
		const start = buf.len();
		buf.writeByteAssumeCapacity('{');
		for (values) |value| {
			writeFn(value, buf);
			buf.writeByteAssumeCapacity(',');
		}

		// strip out last comma
		buf.truncate(1);
		buf.writeByteAssumeCapacity('}');
		// -6 since the oid and the
		view.writeIntBig(i32, @intCast(buf.len() - start));
	}

	// Fairly special case for text-encoded arrays where we _always_ want to quote the value
	// but don't need to escape. This idea is taken from Java's PostgreSQL JDBC driver
	// specificallly for dealing with possible scientific notation in float/numeric text values
	pub fn writeTextEncodedEscapedArray(values: []const []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
		var l: usize = 0;
		for (values) |v| {
			// +2 for the quotes around the value we'll need
			l += v.len + 2;
		}
		return Encode.writeTextEncodedArray(values, l, buf, format_pos, writeQuotedValue);
	}

	fn writeQuotedValue(value: []const u8, buf: *buffer.Buffer) void {
		buf.writeByteAssumeCapacity('"');
		buf.writeAssumeCapacity(value);
		buf.writeByteAssumeCapacity('"');
	}

	// Fairly special case for text-encoded arrays where we _always_ want to quote the value
	// but don't need to escape. This idea is taken from Java's PostgreSQL JDBC driver
	// specificallly for dealing with possible scientific notation in float/numeric text values
	pub fn writeTextEncodedRawArray(values: []const []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
		var l: usize = 0;
		for (values) |v| {
			l += v.len;
		}
		return Encode.writeTextEncodedArray(values, l, buf, format_pos, writeRawValue);
	}

	fn writeRawValue(value: []const u8, buf: *buffer.Buffer) void {
		buf.writeAssumeCapacity(value);
	}

	pub fn writeTextEncodedCharArray(values: []const u8, buf: *buffer.Buffer, format_pos: usize) !void {
		buf.writeAt(&text_encoding, format_pos);
		if (values.len == 0) {
			// empty array, with length prefix
			return buf.write(&.{0, 0, 0, 2, '{', '}'});
		}

		// 6 = 4-byte length + opening brace + closing brace
		// v.len * 5 is the max guess about how much room we'll need. 1 byte
		// per character, delimiter + double quotes + escape
		const estimated_len: usize = 6 + values.len * 5;
		try buf.ensureUnusedCapacity(estimated_len);

		// skip the length, which we'll fill later
		var view = try buf.skip(4);
		const start = buf.len();

			// https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
		buf.writeByteAssumeCapacity('{');
		for (values) |c| {
			if (c == '"' or c == '\\') {
				buf.writeAssumeCapacity("\"\\");
				buf.writeByteAssumeCapacity(c);
				buf.writeByteAssumeCapacity('"');
			} else if (std.ascii.isWhitespace(c) or c == ',' or c == '{' or c == '}' or c == '\\') {
				buf.writeByteAssumeCapacity('"');
				buf.writeByteAssumeCapacity(c);
				buf.writeByteAssumeCapacity('"');
			} else {
				buf.writeByteAssumeCapacity(c);
			}
			buf.writeByteAssumeCapacity(',');
		}

		// strip out last comma
		buf.truncate(1);
		buf.writeByteAssumeCapacity('}');
		view.writeIntBig(i32, @intCast(buf.len() - start));
	}
};

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
				Types.Int16.oid.decimal => {
					if (value > 32767 or value < -32768) return error.IntWontFit;
					return Types.Int16.encode(@intCast(value), buf, format_pos);
				},
				Types.Int32.oid.decimal => {
					if (value > 2147483647 or value < -2147483648) return error.IntWontFit;
					return Types.Int32.encode(@intCast(value), buf, format_pos);
				},
				Types.Timestamp.oid.decimal, Types.TimestampTz.oid.decimal => return Types.Timestamp.encode(@intCast(value), buf, format_pos),
				Types.Numeric.oid.decimal => return Types.Numeric.encode(@as(f64, @floatFromInt(value)), buf, format_pos),
				Types.Char.oid.decimal => {
					if (value > 255 or value < 0) return error.IntWontFit;
					return Types.Char.encode(@intCast(value), buf, format_pos);
				},
				else => return Types.Int64.encode(@intCast(value), buf, format_pos),
			}
		},
		.Int => {
			switch (oid) {
				Types.Int16.oid.decimal => {
					if (value > 32767 or value < -32768) return error.IntWontFit;
					return Types.Int16.encode(@intCast(value), buf, format_pos);
				},
				Types.Int32.oid.decimal => {
					if (value > 2147483647 or value < -2147483648) return error.IntWontFit;
					return Types.Int32.encode(@intCast(value), buf, format_pos);
				},
				Types.Timestamp.oid.decimal, Types.TimestampTz.oid.decimal => return Types.Timestamp.encode(@intCast(value), buf, format_pos),
				Types.Numeric.oid.decimal => return Types.Numeric.encode(@as(f64, @floatFromInt(value)), buf, format_pos),
				Types.Char.oid.decimal => {
					if (value > 255 or value < 0) return error.IntWontFit;
					return Types.Char.encode(@intCast(value), buf, format_pos);
				},
				else => {
					if (value > 9223372036854775807 or value < -9223372036854775808) return error.IntWontFit;
					return Types.Int64.encode(@intCast(value), buf, format_pos);
				},
			}
		},
		.ComptimeFloat => {
			switch (oid) {
				Types.Float32.oid.decimal => return Types.Float32.encode(@floatCast(value), buf, format_pos),
				Types.Numeric.oid.decimal => return Types.Numeric.encode(value, buf, format_pos),
				else => return Types.Float64.encode(@floatCast(value), buf, format_pos),
			}
		},
		.Float => {
			switch (oid) {
				Types.Float32.oid.decimal => return Types.Float32.encode(@floatCast(value), buf, format_pos),
				Types.Numeric.oid.decimal => return Types.Numeric.encode(value, buf, format_pos),
				else => return Types.Float64.encode(@floatCast(value), buf, format_pos),
			}
		},
		.Bool => return Types.Bool.encode(value, buf, format_pos),
		.Pointer => |ptr| {
			switch (ptr.size) {
				.Slice => {
					if (ptr.is_const) {
						return bindSlice(oid, @as([]const ptr.child, value), buf, format_pos);
					} else {
						return bindSlice(oid, @as([]ptr.child, value), buf, format_pos);
					}
				},
				.One => switch (@typeInfo(ptr.child)) {
					.Array => {
						const Slice = []const std.meta.Elem(ptr.child);
						return bindSlice(oid, @as(Slice, value), buf, format_pos);
					},
					.Struct => switch (oid) {
						Types.JSON.oid.decimal => return Types.JSON.encode(value, buf, format_pos),
						Types.JSONB.oid.decimal => return Types.JSONB.encode(value, buf, format_pos),
						else => return error.CannotBindStruct,
					},
					else => compileHaltBindError(T),
				},
				else => compileHaltBindError(T),
			}
		},
		.Array => return bindValue(@TypeOf(&value), oid, &value, buf, format_pos),
		.Struct => return bindValue(@TypeOf(&value), oid, &value, buf, format_pos),
		.Optional => |opt| {
			if (value) |v| {
				return bindValue(opt.child, oid, v, buf, format_pos);
			}
			// null
			return buf.write(&.{255, 255, 255, 255});
		},
		.Enum, .EnumLiteral => return Types.String.encode(@tagName(value), buf, format_pos),
		else => compileHaltBindError(T),
	}
}

fn bindSlice(oid: i32, value: anytype, buf: *buffer.Buffer, format_pos: usize) !void {
	const T = @TypeOf(value);
	if (T == []u8 or T == []const u8) {
		switch (oid) {
			Types.Bytea.oid.decimal => return Types.Bytea.encode(value, buf, format_pos),
			Types.UUID.oid.decimal => return Types.UUID.encode(value, buf, format_pos),
			Types.JSONB.oid.decimal => return Types.JSONB.encodeBytes(value, buf, format_pos),
			Types.JSON.oid.decimal => return Types.JSON.encodeBytes(value, buf, format_pos),
			Types.MacAddr.oid.decimal => return Types.MacAddr.encode(value, buf, format_pos),
			Types.MacAddr8.oid.decimal => return Types.MacAddr8.encode(value, buf, format_pos),
			Types.CharArray.oid.decimal => {
				// This is actually an array, and in theory we could let it fallthrough
				// to the binary-array handling. BUT, if we do that, the code won't compile
				// because it would mean T can be []u8 or []const u8, and that makes parts
				// of the code invalid. Also, encoding a char array using the text protocol
				// is going to be more efficient than encoding it using the binary protocol.
				return Encode.writeTextEncodedCharArray(value, buf, format_pos);
			},
			else => return Types.String.encode(value, buf, format_pos),
		}
	}

	// For now, a few types are text-encoded. This largely has to do with the fact
	// that there's no native Zig type, so a text representation lets us use PG's
	// own text->type conversion.
	if (comptime isStringArray(T)) {
		switch (oid) {
			Types.TimestampArray.oid.decimal,
			Types.NumericArray.oid.decimal => return Encode.writeTextEncodedEscapedArray(value, buf, format_pos),
			Types.TimestampTzArray.oid.decimal,
			Types.CidrArray.oid.decimal,
			Types.CidrArray.inet_oid.decimal => return Encode.writeTextEncodedRawArray(value, buf, format_pos),
			Types.MacAddrArray.oid.decimal => return Types.MacAddrArray.encode(value, buf, format_pos),
			Types.MacAddr8Array.oid.decimal => return Types.MacAddr8Array.encode(value, buf, format_pos),
			else => {}, // fallthrough to binary encoding
		}
	}

	// We have an array. All arrays have the same header. We'll write this into
	// buf now. It's possible we don't support the array type, so this can still
	// fail.

	// arrays are always binary encoded (for now...)

	buf.writeAt(&binary_encoding, format_pos);

	const start_pos = buf.len();

	try buf.write(&.{
		0, 0, 0, 0, // placeholder for the length of this parameter
		0, 0, 0, 1, // number of dimensions, for now, we only support one
		0, 0, 0, 0, // bitmask of null, currently, with a single dimension, we don't have null arrays
		0, 0, 0, 0, // placeholder for the oid of each value
	});

	// where in buf, to write the OID of the values
	const oid_pos = buf.len() - 4;

	// number of values in our first (and currently only) dimension
	try buf.writeIntBig(i32, @intCast(value.len));
	try buf.write(&.{0, 0, 0, 1}); // lower bound of this demension

	const ElemT = @typeInfo(T).Pointer.child;
	switch (@typeInfo(ElemT)) {
		.Int => |int| {
			if (int.signedness == .signed) {
				switch (int.bits) {
					16 => try Types.Int16Array.encode(value, buf, oid_pos),
					32 => try Types.Int32Array.encode(value, buf, oid_pos),
					64 => {
						switch (oid) {
							Types.TimestampArray.oid.decimal => try Types.TimestampArray.encode(value, buf, oid_pos),
							Types.TimestampTzArray.oid.decimal => try Types.TimestampTzArray.encode(value, buf, oid_pos),
							else => try Types.Int64Array.encode(value, buf, oid_pos),
						}
					},
					else => compileHaltBindError(T),
				}
			} else {
				switch (int.bits) {
					8 => try Types.CharArray.encodeOne(value, buf, oid_pos),
					16 => try Types.Int16Array.encodeUnsigned(value, buf, oid_pos),
					32 => try Types.Int32Array.encodeUnsigned(value, buf, oid_pos),
					64 => try Types.Int64Array.encodeUnsigned(value, buf, oid_pos),
					else => compileHaltBindError(T),
				}
			}
		},
		.Float => |float| {
			if (oid == Types.NumericArray.oid.decimal) {
				try Types.NumericArray.encode(value, buf, oid_pos);
			} else switch (float.bits) {
				32 => try Types.Float32Array.encode(value, buf, oid_pos),
				64 => try Types.Float64Array.encode(value, buf, oid_pos),
				else => compileHaltBindError(T),
			}
		},
		.Bool => try Types.BoolArray.encode(value, buf, oid_pos),
		.Pointer => |ptr| switch (ptr.size) {
			.Slice => switch (ptr.child) {
				u8 => switch (oid) {
					Types.StringArray.oid.decimal => try Types.StringArray.encode(value, buf, oid_pos),
					Types.UUIDArray.oid.decimal => try Types.UUIDArray.encode(value, buf, oid_pos),
					Types.JSONBArray.oid.decimal => try Types.JSONBArray.encode(value, buf, oid_pos),
					Types.JSONArray.oid.decimal => try Types.JSONArray.encode(value, buf, oid_pos),
					Types.CharArray.oid.decimal => try Types.CharArray.encode(value, buf, oid_pos),
					// we try this as a default to support user defined types with unknown oids
					// (like an array of enums)
					else => try Types.ByteaArray.encode(value, buf, oid_pos),
				},
				else => compileHaltBindError(T),
			},
			else => compileHaltBindError(T),
		},
		.Enum, .EnumLiteral => try Types.StringArray.encodeEnum(&value, buf, oid_pos),
		.Array => try bindSlice(oid, &value, buf, format_pos),
		else => compileHaltBindError(T),
	}

	var param_len: [4]u8 = undefined;
	// write the lenght of the parameter, -4 because for paremeters, the length
	// prefix itself isn't included.
	std.mem.writeInt(i32, &param_len, @intCast(buf.len() - start_pos - 4), .big);
	buf.writeAt(&param_len, start_pos);
}

fn isStringArray(comptime T: type) bool {
	switch (@typeInfo(T)) {
		.Pointer => |ptr| switch (ptr.size) {
			.Slice => switch (ptr.child) {
				[]u8, []const u8 => return true,
				else => return false,
			},
			else => return false,
		},
		else => return false,
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
	var view = try buf.skip(space_needed);

	view.writeIntBig(u16, @intCast(oids.len));
	for (oids) |oid| {
		view.write(Types.resultEncodingFor(oid));
	}
}

fn compileHaltBindError(comptime T: type) noreturn {
	@compileError("cannot bind value of type " ++ @typeName(T));
}

const t = lib.testing;
test "UUID: toString" {
	try t.expectError(error.InvalidUUID, Types.UUID.toString(&.{73, 190, 142, 9, 170, 250, 176, 16, 73, 21}));

	const s = try Types.UUID.toString(&.{183, 204, 40, 47, 236, 67, 73, 190, 142, 9, 170, 250, 176, 16, 73, 21});
	try t.expectString("b7cc282f-ec43-49be-8e09-aafab0104915", &s);
}

test "UUID: toBytes" {
	try t.expectError(error.InvalidUUID, Types.UUID.toBytes(""));

	{
		const s = try Types.UUID.toBytes("166B4751-D702-4FB9-9A2A-CD6B69ED18D6");
		try t.expectSlice(u8, &.{22, 107, 71, 81, 215, 2, 79, 185, 154, 42, 205, 107, 105, 237, 24, 214}, &s);
	}

	{
		const s = try Types.UUID.toBytes("166b4751-d702-4fb9-9a2a-cd6b69ed18d7");
		try t.expectSlice(u8, &.{22, 107, 71, 81, 215, 2, 79, 185, 154, 42, 205, 107, 105, 237, 24, 215}, &s);
	}
}
