const std = @import("std");

const os = std.os;
const net = std.net;
const tls = std.crypto.tls;

const Allocator = std.mem.Allocator;

pub const Stream = struct {
	stream: net.Stream,
	tls_client: ?tls.Client = null,

	pub fn init(stream: net.Stream, tls_client: ?tls.Client) Stream {
		return .{
			.stream = stream,
			.tls_client = tls_client,
		};
	}

	pub fn close(self: *Stream) void {
		if (self.tls_client) |*tls_client| {
			_ = tls_client.writeEnd(self.stream, "", true) catch {};
		}
		self.stream.close();
	}


	pub fn read(self: *Stream, buf: []u8) !usize {
		if (self.tls_client) |*tls_client| {
			return tls_client.read(self.stream, buf);
		}
		return self.stream.read(buf);
	}

	pub fn writeAll(self: *Stream, data: []const u8) !void {
		if (self.tls_client) |*tls_client| {
			return tls_client.writeAll(self.stream, data);
		}
		return self.stream.writeAll(data);
	}

	pub fn setsockopt(self: *const Stream, optname: u32, value: []const u8) !void {
		return os.setsockopt(self.stream.handle, os.SOL.SOCKET, optname, value);
	}
};
