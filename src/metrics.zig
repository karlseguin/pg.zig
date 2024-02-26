const std = @import("std");
const m = @import("metrics");

pub const RegistryOpts = m.RegistryOpts;

// defaults to noop metrics, making this safe to use
// whether or not initializeMetrics is called
var metrics = m.initializeNoop(Metrics);

const Metrics = struct {
	queries: m.Counter(usize),
	pool_empty: m.Counter(usize),
	pool_dirty: m.Counter(usize),
	alloc_params: m.Counter(u64),
	alloc_reader: m.Counter(u64),
};

pub fn initialize(allocator: std.mem.Allocator, comptime opts: m.RegistryOpts) !void {
	metrics = .{
		.queries = try m.Counter(usize).init(allocator, "pg_query", .{}, opts),
		.pool_empty = try m.Counter(usize).init(allocator, "pg_pool_empty", .{}, opts),
		.pool_dirty = try m.Counter(usize).init(allocator, "pg_pool_dirty", .{}, opts),
		.alloc_params = try m.Counter(u64).init(allocator, "pg_alloc_params", .{}, opts),
		.alloc_reader = try m.Counter(u64).init(allocator, "pg_alloc_reader", .{}, opts),
	};
}

pub fn write(writer: anytype) !void {
	return m.write(metrics, writer);
}

pub fn query() void {
	metrics.queries.incr();
}

pub fn poolEmpty() void {
	metrics.pool_empty.incr();
}

pub fn poolDirty() void {
	metrics.pool_dirty.incr();
}

pub fn allocParams(count: usize) void {
	// this is the # of parameters, not the bytes allocated.
	metrics.alloc_params.incrBy(count);
}

pub fn allocReader(size: usize) void {
	metrics.alloc_reader.incrBy(size);
}
