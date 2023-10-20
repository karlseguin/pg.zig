const std = @import("std");
const Allocator = std.mem.Allocator;

pub const SASL = struct {
	buf: []u8,
	fba: std.heap.FixedBufferAllocator,
	client_first_message: []u8,
	auth_message: ?[]const u8 = null,
	salted_password: ?[32] u8 = null,
	server_response: ?ServerResponse = null,

	const Base64Encoder = std.base64.standard.Encoder;
	const Base64Decoder = std.base64.standard.Decoder;

	pub fn init(allocator: Allocator) !SASL {
		var buf = try allocator.alloc(u8, 1024);
		errdefer allocator.free(buf);

		var fba = std.heap.FixedBufferAllocator.init(buf);

		var nonce: [18]u8 = undefined;
		std.crypto.random.bytes(&nonce);

		var client_first_message = try fba.allocator().alloc(u8, 32);
		client_first_message[0] = 'n';
		client_first_message[1] = ',';
		client_first_message[2] = ',';
		client_first_message[3] = 'n';
		client_first_message[4] = '=';
		client_first_message[5] = ',';
		client_first_message[6] = 'r';
		client_first_message[7] = '=';
		_ = Base64Encoder.encode(client_first_message[8..], &nonce);

		return .{
			.buf = buf,
			.fba = fba,
			.client_first_message = client_first_message,
		};
	}

	pub fn deinit(self: SASL, allocator: Allocator) void {
		allocator.free(self.buf);
	}

	pub fn serverResponse(self: *SASL, data: []const u8) !void {
		if (data.len < 8) {
			return error.InvalidLength;
		}

		// Specification states the attribute positions are fixed, so we expect r=X,s=Y,i=Z
		if (data[0] != 'r' or data[1] != '=') {
			return error.InvalidNoncePrefix;
		}

		const owned = try self.fba.allocator().dupe(u8, data);

		var res = ServerResponse{
			.raw = owned,
			.nonce = undefined,
			.base64_salt = undefined,
			.iterations = undefined,
		};

		var pos: usize = 2;
		{
			const sep = std.mem.indexOfScalarPos(u8, owned, pos, ',') orelse return error.MissingSalt;
			res.nonce = owned[2..sep];
			pos = sep + 1;
		}

		{
			const value_start = pos + 2;
			if (owned.len < value_start or owned[pos] != 's' or owned[pos+1] != '=') {
				return error.InvalidSaltPrefix;
			}
			pos = value_start;

			const sep = std.mem.indexOfScalarPos(u8, owned, pos, ',') orelse return error.MissingIterations;
			res.base64_salt = owned[pos..sep];
			pos = sep + 1;
		}

		{
			const value_start = pos + 2;
			if (owned.len < value_start or owned[pos] != 'i' or owned[pos+1] != '=') {
				return error.InvalidIterationPrefix;
			}
			pos = value_start;
			const sep = std.mem.indexOfScalarPos(u8, owned, pos, ',') orelse owned.len;
			res.iterations = std.fmt.parseInt(u32, owned[pos..sep], 10) catch return error.InvalidIteration;
		}

		self.server_response = res;
	}

	pub fn clientFinalMessage(self: *SASL, password: []const u8) ![]const u8 {
		const sr = self.server_response orelse return error.MissingServerResponse;
		const allocator = self.fba.allocator();

		const salt = blk: {
			const s = try allocator.alloc(u8, try Base64Decoder.calcSizeForSlice(sr.base64_salt));
			try Base64Decoder.decode(s, sr.base64_salt);
			break :blk s;
		};

		const unproved = try std.fmt.allocPrint(allocator, "c=biws,r={s}", .{sr.nonce});
		const auth_message = try std.fmt.allocPrint(allocator, "{s},{s},{s}", .{self.client_first_message[3..], sr.raw, unproved});
		const salted_password = blk: {
			var buf: [32]u8 = undefined;
			try std.crypto.pwhash.pbkdf2(&buf, password, salt, sr.iterations, std.crypto.auth.hmac.sha2.HmacSha256);
			break :blk buf;
		};

		const proof = blk: {
			var client_key: [32]u8 = undefined;
			std.crypto.auth.hmac.sha2.HmacSha256.create(&client_key, "Client Key", &salted_password);

			var stored_key: [32]u8 = undefined;
			std.crypto.hash.sha2.Sha256.hash(&client_key, &stored_key, .{});

			var client_signature: [32]u8 = undefined;
			std.crypto.auth.hmac.sha2.HmacSha256.create(&client_signature, auth_message, &stored_key);

			var proof: [32]u8 = undefined;
			for (client_key, client_signature, 0..) |ck, cs, i| {
				proof[i] = ck ^ cs;
			}

			var encoded_proof: [44]u8 = undefined;
			_ = Base64Encoder.encode(&encoded_proof, &proof);
			break :blk encoded_proof;
		};

		self.auth_message = auth_message;
		self.salted_password = salted_password;
		return std.fmt.allocPrint(allocator, "{s},p={s}", .{unproved, proof});
	}

	pub fn verifyServerFinal(self: *SASL, data: []const u8) !void {
		if (data.len < 46) {
			return error.InvalidLength;
		}
		const auth_message = self.auth_message orelse return error.MissingAutMessage;
		const salted_password = if (self.salted_password) |*sp| sp else return error.MissingSaltedPassword;

		const computed_signature = blk: {
			var server_key: [32]u8 = undefined;
			std.crypto.auth.hmac.sha2.HmacSha256.create(&server_key, "Server Key", salted_password);

			var server_signature: [32]u8 = undefined;
			std.crypto.auth.hmac.sha2.HmacSha256.create(&server_signature, auth_message, &server_key);

			var encoded_signature: [44]u8 = undefined;
			_ = Base64Encoder.encode(&encoded_signature, &server_signature);
			break :blk encoded_signature;
		};

		// don't tell me about timing leaks unless there's also something in std to deal with it
		if (std.mem.eql(u8, &computed_signature, data[2..]) == false) {
			return error.InvalidServerSignature;
		}
	}
};

pub const ServerResponse = struct {
	raw: []const u8,
	nonce: []const u8,
	base64_salt: []const u8,
	iterations: u32,
};

const t = @import("lib.zig").testing;

test "SASL: init" {
	const sasl1 = try SASL.init(t.allocator);
	defer sasl1.deinit(t.allocator);
	try t.expectString("n,,n=,r=", sasl1.client_first_message[0..8]);

	const sasl2 = try SASL.init(t.allocator);
	defer sasl2.deinit(t.allocator);
	try t.expectString("n,,n=,r=", sasl2.client_first_message[0..8]);

	const sasl3 = try SASL.init(t.allocator);
	defer sasl3.deinit(t.allocator);
	try t.expectString("n,,n=,r=", sasl3.client_first_message[0..8]);

	// The nonce should be random. It's unlikely that if we generate 5, we'd get
	// the same value at a given byte.
	const nonce1 = sasl1.client_first_message[8..];
	const nonce2 = sasl2.client_first_message[8..];
	const nonce3 = sasl3.client_first_message[8..];
	const nonce4 = sasl3.client_first_message[8..];
	for (0..18) |i| {
		try t.expectEqual(true,
			nonce1[i] != nonce2[i] or
			nonce2[i] != nonce3[i] or
			nonce1[i] != nonce3[i] or
			nonce3[i] != nonce4[i] or
			nonce1[i] != nonce4[i] or
			nonce2[i] != nonce4[i]
		);
	}
}

test "SASL: serverResponse invalid" {
	//invalid response
	const InvalidTest = struct {
		input: []const u8,
		expected: anyerror,
	};

	const test_cases = [_]InvalidTest{
		.{.input = "", .expected = error.InvalidLength},
		.{.input = "r", .expected = error.InvalidLength},
		.{.input = "r=", .expected = error.InvalidLength},
		.{.input = "s=abc,r=123,i=32", .expected = error.InvalidNoncePrefix},
		.{.input = "r=abc123,i=32,s=aaa", .expected = error.InvalidSaltPrefix},
		.{.input = "r=abc123,s=aaa,x=32", .expected = error.InvalidIterationPrefix},
		.{.input = "r=abc123", .expected = error.MissingSalt},
		.{.input = "r=abc123,s=aaaa", .expected = error.MissingIterations},
		.{.input = "r=abc123,s=aaaa,i=123a", .expected = error.InvalidIteration},
	};

	var sasl = try SASL.init(t.allocator);
	defer sasl.deinit(t.allocator);
	for (test_cases) |tc| {
		try t.expectError(tc.expected, sasl.serverResponse(tc.input));
		try t.expectEqual(null, sasl.server_response);
	}
}

test "SASL: serverResponse" {
	var sasl = try SASL.init(t.allocator);
	defer sasl.deinit(t.allocator);

	try sasl.serverResponse("r=abc123,s=aaaaxa,i=4096");
	try t.expectString("abc123", sasl.server_response.?.nonce);
	try t.expectString("aaaaxa", sasl.server_response.?.base64_salt);
	try t.expectEqual(4096, sasl.server_response.?.iterations);
}

// not deterministic, tested via integration test of actually authentication to pg
// test "SASL: clientFinalMessage" {}
