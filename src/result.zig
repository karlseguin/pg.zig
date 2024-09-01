const std = @import("std");
const lib = @import("lib.zig");

const types = lib.types;
const proto = lib.proto;
const Conn = lib.Conn;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const Result = struct {
    number_of_columns: usize,

    // will be empty unless the query was executed with the column_names = true option
    column_names: [][]const u8,

    _conn: *Conn,
    _arena: *ArenaAllocator,

    // a sliced version of _state.oids (so we don't have to keep reslicing it to
    // number_of_columns on each row)
    _oids: []i32,

    // a sliced version of _state.values (so we don't have to keep reslicing it to
    // number_of_columns on each row)
    _values: []State.Value,

    // When true, result.deinit() will call conn.release()
    // Used when the result came directly from the pool.query() helper.
    _release_conn: bool,

    pub fn deinit(self: *const Result) void {
        // value.data references the buffer of the reader, this buffer is potentially
        // reused and potentially discarded. There are at least a few very good
        // reasons why the least we can do is blank it out.
        for (self._values) |*value| {
            value.data = &[_]u8{};
        }

        self._conn._reader.endFlow() catch {
            // this can only fail in extreme conditions (OOM) and it will only impact
            // the next query (and if the app is using the pool, the pool will try to
            // recover from this anyways)
            self._conn._state = .fail;
        };

        if (self._release_conn) {
            self._conn.release();
        }

        const arena = self._arena;
        const allocator = arena.child_allocator;
        arena.deinit();
        allocator.destroy(arena);
    }

    // Caller should typically call next() until null is returned.
    // But in some cases, that might not be desirable. So they can
    // "drain" to empty the rest of the result.
    // I don't want to do this implictly in deinit because it can fail
    // and returning an error union in deinit is a pain for the caller.
    pub fn drain(self: *Result) !void {
        var conn = self._conn;
        if (conn._state == .idle) {
            return;
        }

        while (true) {
            const msg = try conn.read();
            switch (msg.type) {
                'C' => {}, // CommandComplete
                'D' => {}, // DataRow
                'Z' => return,
                else => return error.UnexpectedDBMessage,
            }
        }
    }

    pub fn next(self: *Result) !?Row {
        if (self._conn._state != .query) {
            // Possibly weird state. Most likely cause is calling next() multiple times
            // despite null being returned.
            return null;
        }

        const msg = try self._conn.read();
        switch (msg.type) {
            'D' => {
                const data = msg.data;
                // Since our Row API gets data by column #, we need translate the column
                // # to a slice within msg.data. We could do this on the fly within Row,
                // but creating this mapping up front simplifies things and, in normal
                // cases, performs best. "Normal case" here assumes that the client app
                // is going to fetch most/all columns.

                // first column starts at position 2
                var offset: usize = 2;
                const values = self._values;
                for (values) |*value| {
                    const data_start = offset + 4;
                    const length = std.mem.readInt(i32, data[offset..data_start][0..4], .big);
                    if (length == -1) {
                        value.is_null = true;
                        value.data = &[_]u8{};
                        offset = data_start;
                    } else {
                        const data_end = data_start + @as(usize, @intCast(length));
                        value.is_null = false;
                        value.data = data[data_start..data_end];
                        offset = data_end;
                    }
                }

                return .{
                    .values = values,
                    .oids = self._oids,
                    ._result = self,
                };
            },
            'C' => {
                try self._conn.readyForQuery();
                return null;
            },
            else => return error.UnexpectedDBMessage,
        }
    }

    pub fn columnIndex(self: *const Result, column_name: []const u8) ?usize {
        for (self.column_names, 0..) |n, i| {
            if (std.mem.eql(u8, n, column_name)) {
                return i;
            }
        }
        return null;
    }

    const MapperOpts = struct {
        dupe: bool = false,
        allocator: ?Allocator = null,
    };

    pub fn mapper(self: *Result, comptime T: type, opts: MapperOpts) Mapper(T) {
        var column_indexes: [std.meta.fields(T).len]?usize = undefined;

        inline for (std.meta.fields(T), 0..) |field, i| {
            column_indexes[i] = self.columnIndex(field.name);
        }

        // if we're given an allocator, use that.
        // if we're not given an allocator, but asked to dupe use our arena and thus
        // tie the lifetime of the returned T to the lifetime of the DB result object.
        var allocator: ?Allocator = null;
        if (opts.allocator) |a| {
            allocator = a;
        } else if (opts.dupe) {
            allocator = self._arena.allocator();
        }

        return .{
            .result = self,
            .allocator = allocator,
            .column_indexes = column_indexes,
        };
    }

    // For every query, we need to store the type of each column (so we know
    // how to parse the data). Optionally, we might need the name of each column.
    // The connection has a default Result.STate for a max # of columns, and we'll use
    // that whenever we can. Otherwise, we'll create this dynamically.
    pub const State = struct {
        // The name for each returned column, we only populate this if we're told
        // to (since it requires us to dupe the data)
        names: [][]const u8,

        // This is different than the above. The above are set once per query
        // from the RowDescription response of our Describe message. This is set for
        // each DataRow message we receive. It maps a column position with the encoded
        // value.
        values: []Value,

        // The OID for each returned column
        oids: []i32,

        pub const Value = struct {
            is_null: bool,
            data: []const u8,
        };

        pub fn init(allocator: Allocator, size: usize) !State {
            const names = try allocator.alloc([]u8, size);
            errdefer allocator.free(names);

            const values = try allocator.alloc(Value, size);
            errdefer allocator.free(values);

            const oids = try allocator.alloc(i32, size);
            errdefer allocator.free(oids);

            return .{
                .names = names,
                .values = values,
                .oids = oids,
            };
        }

        // Populates the State from the RowDescription payload
        // We already read the number_of_columns from data, so we pass it in here
        // We also already know that number_of_columns fits within our arrays
        pub fn from(self: *State, number_of_columns: u16, data: []const u8, allocator: ?Allocator) !void {
            // skip the column count, which we already know as number_of_columns
            var pos: usize = 2;

            for (0..number_of_columns) |i| {
                const end_pos = std.mem.indexOfScalarPos(u8, data, pos, 0) orelse return error.InvalidDataRow;
                if (data.len < (end_pos + 19)) {
                    return error.InvalidDataRow;
                }
                if (allocator) |a| {
                    self.names[i] = try a.dupe(u8, data[pos..end_pos]);
                }

                // skip the name null terminator (1)
                // skip the table object_id this table belongs to (4)
                // skip the attribute number of this table column (2)
                pos = end_pos + 7;

                {
                    const end = pos + 4;
                    self.oids[i] = std.mem.readInt(i32, data[pos..end][0..4], .big);
                    pos = end;
                }

                // skip date type size (2), type modifier (4) format code (2)
                pos += 8;
            }
        }

        pub fn deinit(self: State, allocator: Allocator) void {
            allocator.free(self.names);
            allocator.free(self.values);
            allocator.free(self.oids);
        }
    };
};

pub const Row = struct {
    _result: *Result,
    oids: []i32,
    values: []Result.State.Value,

    pub fn get(self: *const Row, comptime T: type, col: usize) T {
        const value = self.values[col];
        const TT = switch (@typeInfo(T)) {
            .optional => |opt| {
                if (value.is_null) {
                    return null;
                } else {
                    return self.get(opt.child, col);
                }
            },
            .@"struct" => blk: {
                if (@hasDecl(T, "fromPgzRow") == true) {
                    return T.fromPgzRow(value, self.oids[col]);
                }
                break :blk T;
            },
            else => blk: {
                lib.assertNotNull(T, value.is_null);
                break :blk T;
            },
        };

        return getScalar(TT, value.data, self.oids[col]);
    }

    pub fn getCol(self: *const Row, comptime T: type, name: []const u8) T {
        const col = self._result.columnIndex(name);
        lib.assertColumnName(name, col != null);
        return self.get(T, col.?);
    }

    pub fn iterator(self: *const Row, comptime T: type, col: usize) IteratorReturnType(T) {
        const value = self.values[col];
        const TT = switch (@typeInfo(T)) {
            .optional => |opt| if (value.is_null) return null else opt.child,
            else => T,
        };
        return Iterator(TT).fromPgzRow(value, self.oids[col]);
    }

    pub fn iteratorCol(self: *const Row, comptime T: type, name: []const u8) IteratorReturnType(T) {
        const col = self._result.columnIndex(name);
        lib.assertColumnName(name, col != null);
        return self.iterator(T, col.?);
    }

    pub fn record(self: *const Row, col: usize) Record {
        const data = self.values[col].data;
        const number_of_columns = std.mem.readInt(i32, data[0..4], .big);
        return .{
            .data = data[4..],
            .number_of_columns = @intCast(number_of_columns),
        };
    }

    pub fn recordCol(self: *const Row, name: []const u8) Record {
        const col = self._result.columnIndex(name);
        lib.assertColumnName(name, col != null);
        return self.record(col);
    }

    const ToOpts = struct {
        dupe: bool = false,
        map: Mapping = .ordinal,
        allocator: ?Allocator = null,

        const Mapping = enum {
            name,
            ordinal,
        };
    };

    pub fn to(self: *const Row, T: type, opts: ToOpts) !T {
        // if we're given an allocator, use that.
        // if we're not given an allocator, but asked to dupe use our arena and thus
        // tie the lifetime of the returned T to the lifetime of the DB result object.
        var allocator: ?Allocator = null;
        if (opts.allocator) |a| {
            allocator = a;
        } else if (opts.dupe) {
            allocator = self._result._arena.allocator();
        }

        return switch (opts.map) {
            .ordinal => self.toUsingOrdinal(T, allocator),
            .name => return self.toUsingName(T, allocator),
        };
    }

    fn toUsingOrdinal(self: *const Row, T: type, allocator: ?Allocator) !T {
        var value: T = undefined;

        inline for (std.meta.fields(T), 0..) |field, column_index| {
            const column_value = self.get(field.type, column_index);
            @field(value, field.name) = try toValue(field.type, column_value, allocator);
        }
        return value;
    }

    fn toUsingName(self: *const Row, T: type, allocator: ?Allocator) !T {
        var value: T = undefined;
        const result = self._result;
        inline for (std.meta.fields(T)) |field| {
            const name = field.name;
            if (result.columnIndex(name)) |column_index| {
                const column_value = self.get(field.type, column_index);
                @field(value, name) = try toValue(field.type, column_value, allocator);
            } else if (field.default_value) |dflt| {
                @field(value, name) = @as(*align(1) const field.type, @ptrCast(dflt)).*;
            } else {
                return error.FieldColumnMismatch;
            }
        }

        return value;
    }
};

pub fn Mapper(comptime T: type) type {
    return struct {
        result: *Result,
        allocator: ?Allocator,
        column_indexes: [std.meta.fields(T).len]?usize,

        const Self = @This();

        pub fn next(self: *const Self) !?T {
            const row = (try self.result.next()) orelse return null;

            var value: T = undefined;

            const allocator = self.allocator;
            inline for (std.meta.fields(T), self.column_indexes) |field, optional_column_index| {
                const name = field.name;
                if (optional_column_index) |column_index| {
                    const column_value = row.get(field.type, column_index);
                    @field(value, name) = try toValue(field.type, column_value, allocator);
                } else if (field.default_value) |dflt| {
                    @field(value, name) = @as(*align(1) const field.type, @ptrCast(dflt)).*;
                } else {
                    return error.FieldColumnMismatch;
                }
            }

            return value;
        }
    };
}

fn toValue(comptime T: type, value: T, allocator: ?Allocator) !T {
    const a = allocator orelse return value;

    if (@typeInfo(T) == .optional) {
        if (value) |v| {
            return try toValue(@TypeOf(v), v, allocator);
        } else {
            return null;
        }
    }

    if (T == []u8 or T == []const u8) {
        return try a.dupe(u8, value);
    }

    if (std.meta.hasFn(T, "pgzMoveOwner")) {
        return value.pgzMoveOwner(a);
    }

    return value;
}

pub const QueryRow = struct {
    row: Row,
    result: *Result,

    pub fn get(self: *const QueryRow, comptime T: type, col: usize) T {
        return self.row.get(T, col);
    }

    pub fn getCol(self: *const QueryRow, comptime T: type, name: []const u8) T {
        return self.row.getCol(T, name);
    }

    pub fn iterator(self: *const QueryRow, comptime T: type, col: usize) IteratorReturnType(T) {
        return self.row.iterator(T, col);
    }

    pub fn iteratorCol(self: *const QueryRow, comptime T: type, name: []const u8) IteratorReturnType(T) {
        return self.row.iteratorCol(T, name);
    }

    pub fn record(self: *const QueryRow, col: usize) Record {
        return self.row.record(col);
    }

    pub fn recordCol(self: *const QueryRow, name: []const u8) Record {
        return self.row.recordCol(name);
    }
    pub fn to(self: *const QueryRow, T: type, opts: Row.ToOpts) !T {
        return self.row.to(T, opts);
    }

    pub fn deinit(self: *QueryRow) !void {
        // this is unfortunate
        try self.result.drain();
        self.result.deinit();
    }
};

fn IteratorReturnType(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .optional => |opt| ?Iterator(opt.child),
        else => Iterator(T),
    };
}

pub fn Iterator(comptime T: type) type {
    return struct {
        _len: usize,
        _pos: usize,
        _data: []const u8,
        _decoder: *const fn (data: []const u8) ItemType(),

        fn ItemType() type {
            return switch (@typeInfo(T)) {
                .optional => |opt| opt.child,
                else => T,
            };
        }

        const Self = @This();

        pub fn len(self: Self) usize {
            return self._len;
        }

        // used internally by row.get(Iterator(T))
        fn fromPgzRow(value: Result.State.Value, oid: i32) Self {
            const TT = switch (@typeInfo(T)) {
                .optional => |opt| opt.child,
                else => T,
            };

            const decoder = switch (TT) {
                u8 => blk: {
                    lib.assertDecodeType([]u8, &.{types.CharArray.oid.decimal}, oid);
                    break :blk &types.Char.decodeKnown;
                },
                i16 => blk: {
                    lib.assertDecodeType([]i16, &.{types.Int16Array.oid.decimal}, oid);
                    break :blk &types.Int16.decodeKnown;
                },
                i32 => blk: {
                    lib.assertDecodeType([]i32, &.{types.Int32Array.oid.decimal}, oid);
                    break :blk &types.Int32.decodeKnown;
                },
                i64 => switch (oid) {
                    types.TimestampArray.oid.decimal => &types.Timestamp.decodeKnown,
                    types.TimestampTzArray.oid.decimal => &types.Timestamp.decodeKnown,
                    types.Int64Array.oid.decimal => &types.Int64.decodeKnown,
                    else => std.debug.panic("{d} oid cannot target i64 iterator", .{oid}),
                },
                f32 => blk: {
                    lib.assertDecodeType([]f32, &.{types.Float32Array.oid.decimal}, oid);
                    break :blk &types.Float32.decodeKnown;
                },
                f64 => switch (oid) {
                    types.Float64Array.oid.decimal => &types.Float64.decodeKnown,
                    types.NumericArray.oid.decimal => &types.Numeric.decodeKnownToFloat,
                    else => std.debug.panic("{d} oid cannot target f64 iterator", .{oid}),
                },
                bool => blk: {
                    lib.assertDecodeType([]bool, &.{types.BoolArray.oid.decimal}, oid);
                    break :blk &types.Bool.decodeKnown;
                },
                []const u8 => switch (oid) {
                    types.JSONBArray.oid.decimal => &types.JSONB.decodeKnown,
                    else => &types.Bytea.decodeKnown,
                },
                []u8 => switch (oid) {
                    types.JSONBArray.oid.decimal => &types.JSONB.decodeKnownMutable,
                    else => &types.Bytea.decodeKnownMutable,
                },
                types.Numeric => blk: {
                    lib.assertDecodeType([]f64, &.{types.NumericArray.oid.decimal}, oid);
                    break :blk &types.Numeric.decodeKnown;
                },
                types.Cidr => blk: {
                    lib.assertDecodeType([]types.Cidr, &.{types.CidrArray.oid.decimal, types.CidrArray.inet_oid.decimal}, oid);
                    break :blk &types.Cidr.decodeKnown;
                },
                else => compileHaltGetError(T),
            };

            const data = value.data;
            if (data.len == 12) {
                // we have an empty
                return .{
                    ._len = 0,
                    ._pos = 0,
                    ._data = &[_]u8{},
                    ._decoder = decoder,
                };
            }

            // minimum size for 1 empty array
            lib.assert(data.len >= 20);
            const dimensions = std.mem.readInt(i32, data[0..4], .big);
            lib.assert(dimensions == 1);

            const has_nulls = std.mem.readInt(i32, data[4..8][0..4], .big);
            lib.assert(has_nulls == 0);

            // const oid = std.mem.readInt(i32, data[8..12][0..4], .big);
            const l = std.mem.readInt(i32, data[12..16][0..4], .big);
            // const lower_bound = std.mem.readInt(i32, data[16..20][0..4], .big);

            return .{
                ._len = @intCast(l),
                ._pos = 0,
                ._data = data[20..],
                ._decoder = decoder,
            };
        }

        pub fn pgzMoveOwner(self: Self, allocator: Allocator) !Self {
            return .{
                ._len = self._len,
                ._pos = self._pos,
                ._data = try allocator.dupe(u8, self._data),
                ._decoder = self._decoder,
            };
        }

        // Should only be called if the Iterator was created with row.to(...)
        // or a result mapper AND an explicit allocator was given
        pub fn deinit(self: *const Self, allocator: Allocator) void {
            allocator.free(self._data);
        }

        pub fn next(self: *Self) ?T {
            const pos = self._pos;
            const data = self._data;
            if (pos == data.len) {
                return null;
            }

            // TODO: for fixed length types, we don't need to decode the length
            const len_end = pos + 4;
            const value_len = std.mem.readInt(i32, data[pos..len_end][0..4], .big);

            const data_end = len_end + @as(usize, @intCast(value_len));
            lib.assert(data.len >= data_end);

            self._pos = data_end;
            return self._decoder(data[len_end..data_end]);
        }

        pub fn alloc(self: Self, allocator: Allocator) ![]T {
            const into = try allocator.alloc(T, self._len);
            self.fill(into);
            return into;
        }

        pub fn fill(self: Self, into: []T) void {
            const data = self._data;
            const decoder = self._decoder;

            var pos: usize = 0;
            const limit = @min(into.len, self._len);
            for (0..limit) |i| {
                // TODO: for fixed length types, we don't need to decode the length
                const len_end = pos + 4;
                const data_len = std.mem.readInt(i32, data[pos..len_end][0..4], .big);
                pos = len_end + @as(usize, @intCast(data_len));
                into[i] = decoder(data[len_end..pos]);
            }
        }
    };
}

fn compileHaltGetError(comptime T: type) noreturn {
    @compileError("cannot get value of type " ++ @typeName(T));
}

const Record = struct {
    data: []const u8,
    number_of_columns: usize,

    pub fn next(self: *Record, comptime T: type) T {
        var data = self.data;

        // at least 4 bytes for the type and 4 bytes for the lenght
        lib.assert(data.len >= 8);

        const oid = std.mem.readInt(i32, data[0..4], .big);

        data = data[4..];
        const len = std.mem.readInt(i32, data[0..4], .big);

        const TT = switch (@typeInfo(T)) {
            .optional => |opt| blk: {
                if (len == -1) return null;
                break :blk opt.child;
            },
            else => T,
        };

        // end of the data for this "column"
        const end = @as(usize, @intCast(len)) + 4;

        // the rest of the data
        self.data = data[end..];

        // start at 4 to skip the length which we already read
        return getScalar(TT, data[4..end], oid);
    }
};

fn getScalar(T: type, data: []const u8, oid: i32) T {
    switch (T) {
        u8 => return types.Char.decode(data, oid),
        i16 => return types.Int16.decode(data, oid),
        i32 => return types.Int32.decode(data, oid),
        i64 => return types.Int64.decode(data, oid),
        f32 => return types.Float32.decode(data, oid),
        f64 => return types.Float64.decode(data, oid),
        bool => return types.Bool.decode(data, oid),
        []const u8 => return types.Bytea.decode(data, oid),
        []u8 => return @constCast(types.Bytea.decode(data, oid)),
        types.Numeric => return types.Numeric.decode(data, oid),
        types.Cidr => return types.Cidr.decode(data, oid),
        else => switch (@typeInfo(T)) {
            .@"enum" => {
                const str = types.Bytea.decode(data, oid);
                return std.meta.stringToEnum(T, str).?;
            },
            else => compileHaltGetError(T),
        },
    }
}

const t = lib.testing;
test "Result: eager error" {
    var c = t.connect(.{});
    defer c.deinit();

    {
        // Some errors happen when the prepared statement is executed
        try t.expectError(error.PG, c.query("select * from invalid", .{}));
        try t.expectString("relation \"invalid\" does not exist", c.err.?.message);
    }

    {
        // some errors only happen whemn the result is read
        try c.begin();
        defer c.rollback() catch {};
        const sql = "create temp table test1 (id int) on commit drop";
        _ = try c.exec(sql, .{});
        try t.expectError(error.PG, c.query(sql, .{}));
    }

}

test "Result: ints" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::smallint, $2::int, $3::bigint";

    {
        // int max
        var result = try c.query(sql, .{ @as(i16, 32767), @as(i32, 2147483647), @as(i64, 9223372036854775807) });
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
        var result = try c.query(sql, .{ @as(i16, -32768), @as(i32, -2147483648), @as(i64, -9223372036854775808) });
        defer result.deinit();
        const row = (try result.next()).?;
        try t.expectEqual(-32768, row.get(i16, 0));
        try t.expectEqual(-2147483648, row.get(i32, 1));
        try t.expectEqual(-9223372036854775808, row.get(i64, 2));
        try result.drain();
    }

    {
        // int null
        var result = try c.query(sql, .{ null, null, null });
        defer result.deinit();
        defer result.drain() catch unreachable;
        const row = (try result.next()).?;
        try t.expectEqual(null, row.get(?i16, 0));
        try t.expectEqual(null, row.get(?i32, 1));
        try t.expectEqual(null, row.get(?i64, 2));
    }

    {
        // uint within limit
        var result = try c.query(sql, .{ @as(u16, 32767), @as(u32, 2147483647), @as(u64, 9223372036854775807) });
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
        try t.expectError(error.IntWontFit, c.query(sql, .{ @as(u16, 32768), @as(u32, 0), @as(u64, 0) }));
        // u32 outside of limit
        try t.expectError(error.IntWontFit, c.query(sql, .{ @as(u16, 0), @as(u32, 2147483648), @as(u64, 0) }));
        // u64 outside of limit
        try t.expectError(error.IntWontFit, c.query(sql, .{ @as(u16, 0), @as(u32, 0), @as(u64, 9223372036854775808) }));
    }
}

test "Result: floats" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::float4, $2::float8";

    {
        // positive float
        var result = try c.query(sql, .{ @as(f32, 1.23456), @as(f64, 1093.229183) });
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
        var result = try c.query(sql, .{ @as(f32, -392.31), @as(f64, -99991.99992) });
        defer result.deinit();
        const row = (try result.next()).?;
        try t.expectEqual(-392.31, row.get(f32, 0));
        try t.expectEqual(-99991.99992, row.get(f64, 1));
        try t.expectEqual(null, result.next());
    }

    {
        // null float
        var result = try c.query(sql, .{ null, null });
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

test "Result: text and bytea" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::text, $2::bytea";

    {
        // empty
        var result = try c.query(sql, .{ "", "" });
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
        var result = try c.query(sql, .{ "it's over 9000!!!", "i will Not fear" });
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
        var result = try c.query(sql, .{ [_]u8{ 'a', 'c', 'b' }, [_]u8{ 'z', 'z', '3' } });
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
        const s1 = try t.allocator.alloc(u8, 4);
        defer t.allocator.free(s1);
        @memcpy(s1, "Leto");

        var result = try c.query(sql, .{ s1, constString() });
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
        var result = try c.query(sql, .{ null, null });
        defer result.deinit();
        const row = (try result.next()).?;
        try t.expectEqual(null, row.get(?[]u8, 0));
        try t.expectEqual(null, row.get(?[]u8, 1));
        try result.drain();
    }
}

fn constString() []const u8 {
    return "Ghanima";
}

test "Result: optional" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::int, $2::int";

    {
        // int max
        var result = try c.query(sql, .{ @as(?i32, 321), @as(?i32, null) });
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
        // empty row.iterator()
        var result = try c.query("select $1::int[]", .{[_]i32{}});
        defer result.deinit();
        var row = (try result.next()).?;

        var iterator = row.iterator(i32, 0);
        try t.expectEqual(0, iterator.len());

        try t.expectEqual(null, iterator.next());
        try t.expectEqual(null, iterator.next());

        const a = try iterator.alloc(t.allocator);
        try t.expectEqual(0, a.len);
        try result.drain();
    }

    {
        // empty row.get()
        var result = try c.query("select $1::int[]", .{[_]i32{}});
        defer result.deinit();
        var row = (try result.next()).?;

        var iterator = row.get(Iterator(i32), 0);
        try t.expectEqual(0, iterator.len());

        try t.expectEqual(null, iterator.next());
        try t.expectEqual(null, iterator.next());

        const a = try iterator.alloc(t.allocator);
        try t.expectEqual(0, a.len);
        try result.drain();
    }

    {
        // one: row.iterator
        var result = try c.query("select $1::int[]", .{[_]i32{9}});
        defer result.deinit();
        var row = (try result.next()).?;

        var iterator = row.iterator(i32, 0);
        try t.expectEqual(1, iterator.len());

        try t.expectEqual(9, iterator.next());
        try t.expectEqual(null, iterator.next());

        const arr = try iterator.alloc(t.allocator);
        defer t.allocator.free(arr);
        try t.expectEqual(1, arr.len);
        try t.expectSlice(i32, &.{9}, arr);
        try result.drain();
    }

    {
        // one: row.get
        var result = try c.query("select $1::int[]", .{[_]i32{9}});
        defer result.deinit();
        var row = (try result.next()).?;

        var iterator = row.get(Iterator(i32), 0);
        try t.expectEqual(1, iterator.len());

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
        var result = try c.query("select $1::int[]", .{[_]i32{ 0, -19 }});
        defer result.deinit();
        var row = (try result.next()).?;

        var iterator = row.iterator(i32, 0);
        try t.expectEqual(2, iterator.len());

        try t.expectEqual(0, iterator.next());
        try t.expectEqual(-19, iterator.next());
        try t.expectEqual(null, iterator.next());

        var arr1: [2]i32 = undefined;
        iterator.fill(&arr1);
        try t.expectSlice(i32, &.{ 0, -19 }, &arr1);
        try result.drain();

        // smaller
        var arr2: [1]i32 = undefined;
        iterator.fill(&arr2);
        try t.expectSlice(i32, &.{0}, &arr2);
        try result.drain();
    }
}

test "Result: null iterator" {
    var c = t.connect(.{});
    defer c.deinit();

    {
        // null int
        var result = try c.query("select $1::int[]", .{null});
        defer result.deinit();

        var row = (try result.next()).?;

        const iterator = row.iterator(?i32, 0);
        try t.expectEqual(null, iterator);
        try result.drain();
    }

    {
        // null text
        var result = try c.query("select $1::text[]", .{null});
        defer result.deinit();

        var row = (try result.next()).?;

        const iterator = row.iterator(?[]u8, 0);
        try t.expectEqual(null, iterator);
        try result.drain();
    }
}

test "Result: int[]" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::smallint[], $2::int[], $3::bigint[]";

    var result = try c.query(sql, .{ [_]i16{ -303, 9449, 2 }, [_]i32{ -3003, 49493229, 0 }, [_]i64{ 944949338498392, -2 } });
    defer result.deinit();

    var row = (try result.next()).?;

    const v1 = try row.iterator(i16, 0).alloc(t.allocator);
    defer t.allocator.free(v1);
    try t.expectSlice(i16, &.{ -303, 9449, 2 }, v1);

    const v2 = try row.iterator(i32, 1).alloc(t.allocator);
    defer t.allocator.free(v2);
    try t.expectSlice(i32, &.{ -3003, 49493229, 0 }, v2);

    const v3 = try row.iterator(i64, 2).alloc(t.allocator);
    defer t.allocator.free(v3);
    try t.expectSlice(i64, &.{ 944949338498392, -2 }, v3);

    // row 1, but fetch it as a nullable
    const v4 = try row.iterator(?i16, 0).?.alloc(t.allocator);
    defer t.allocator.free(v4);
    try t.expectSlice(i16, &.{ -303, 9449, 2 }, v4);
}

test "Result: float[]" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::float4[], $2::float8[]";

    var result = try c.query(sql, .{ [_]f32{ 1.1, 0, -384.2 }, [_]f64{ -888585.123322, 0.001 } });
    defer result.deinit();

    var row = (try result.next()).?;

    const v1 = try row.iterator(f32, 0).alloc(t.allocator);
    defer t.allocator.free(v1);
    try t.expectSlice(f32, &.{ 1.1, 0, -384.2 }, v1);

    const v2 = try row.iterator(f64, 1).alloc(t.allocator);
    defer t.allocator.free(v2);
    try t.expectSlice(f64, &.{ -888585.123322, 0.001 }, v2);
}

test "Result: bool[]" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::bool[]";

    var result = try c.query(sql, .{[_]bool{ true, false, false }});
    defer result.deinit();

    var row = (try result.next()).?;

    const v1 = try row.iterator(bool, 0).alloc(t.allocator);
    defer t.allocator.free(v1);
    try t.expectSlice(bool, &.{ true, false, false }, v1);
}

test "Result: text[] & bytea[]" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::text[], $2::bytea[]";

    var arr1 = [_]u8{ 0, 1, 2 };
    var arr2 = [_]u8{255};
    var result = try c.query(sql, .{ [_][]const u8{ "over", "9000" }, [_][]u8{ &arr1, &arr2 } });
    defer result.deinit();

    var row = (try result.next()).?;

    const v1 = try row.iterator([]u8, 0).alloc(t.allocator);
    defer t.allocator.free(v1);
    try t.expectString("over", v1[0]);
    try t.expectString("9000", v1[1]);
    try t.expectEqual(2, v1.len);

    const v2 = try row.iterator([]const u8, 1).alloc(t.allocator);
    defer t.allocator.free(v2);
    try t.expectString(&arr1, v2[0]);
    try t.expectString(&arr2, v2[1]);
    try t.expectEqual(2, v2.len);

    // column 0 but fetched as nullable
    const v3 = try row.iterator(?[]u8, 0).?.alloc(t.allocator);
    defer t.allocator.free(v3);
    try t.expectString("over", v3[0]);
    try t.expectString("9000", v3[1]);
    try t.expectEqual(2, v3.len);
}

test "Result: UUID" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select $1::uuid, $2::uuid";
    var result = try c.query(sql, .{ "fcbebf0f-b996-43b9-9818-672bc689cda8", &[_]u8{ 174, 47, 71, 95, 128, 112, 65, 183, 186, 51, 134, 187, 168, 137, 123, 222 } });
    defer result.deinit();

    const row = (try result.next()).?;
    try t.expectSlice(u8, &.{ 252, 190, 191, 15, 185, 150, 67, 185, 152, 24, 103, 43, 198, 137, 205, 168 }, row.get([]u8, 0));
    try t.expectSlice(u8, &.{ 174, 47, 71, 95, 128, 112, 65, 183, 186, 51, 134, 187, 168, 137, 123, 222 }, row.get([]u8, 1));
}

test "Row: column names" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select 923 as id, 'Leto' as name";
    var row = (try c.rowOpts(sql, .{}, .{ .column_names = true })).?;
    defer row.deinit() catch {};

    try t.expectEqual(923, row.getCol(i32, "id"));
    try t.expectString("Leto", row.getCol([]u8, "name"));
}

test "Result: mutable []u8" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select 'Leto'";
    var row = (try c.row(sql, .{})).?;
    defer row.deinit() catch {};

    var name = row.get([]u8, 0);
    name[3] = '!';
    try t.expectString("Let!", name);
}

test "Result: mutable [][]u8" {
    var c = t.connect(.{});
    defer c.deinit();
    const sql = "select array['Leto', 'Test']::text[]";
    var row = (try c.row(sql, .{})).?;
    defer row.deinit() catch {};

    var values = try row.iterator([]u8, 0).alloc(t.allocator);
    defer t.allocator.free(values);
    values[0][0] = 'n';
    try t.expectString("neto", values[0]);
}

test "Row.to: ordinal" {
    const User = struct {
        id: i32,
        active: bool,
        name: []const u8,
        note: ?[]const u8,
        choice: Choice,

        const Choice = enum {
            blue,
            green,
            red,
        };
    };

    var c = t.connect(.{});
    defer c.deinit();

    {
        // null, no dupe
        var row = (try c.row("select 1::integer, true, 'teg', null::text, 'blue'", .{})).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{});
        try t.expectEqual(1, user.id);
        try t.expectEqual(true, user.active);
        try t.expectString("teg", user.name);
        try t.expectEqual(null, user.note);
        try t.expectEqual(.blue, user.choice);
    }

    {
        // not null, no dupe
        var row = (try c.row("select 2::integer, false, 'ghanima', 'n1', 'red'", .{})).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{});
        try t.expectEqual(2, user.id);
        try t.expectEqual(false, user.active);
        try t.expectString("ghanima", user.name);
        try t.expectString("n1", user.note.?);
        try t.expectEqual(.red, user.choice);
    }

    {
        // null, dupe with internal arena
        var row = (try c.row("select 1::integer, true, 'teg', null::text, 'red'", .{})).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .dupe = true });
        try t.expectEqual(1, user.id);
        try t.expectEqual(true, user.active);
        try t.expectString("teg", user.name);
        try t.expectEqual(null, user.note);
        try t.expectEqual(.red, user.choice);
    }

    {
        // not null, dupe with internal arena
        var row = (try c.row("select 2::integer, false, 'ghanima', 'n1', 'red'", .{})).?;
        const user = try row.to(User, .{ .dupe = true });
        defer row.deinit() catch {};

        try t.expectEqual(2, user.id);
        try t.expectEqual(false, user.active);
        try t.expectString("ghanima", user.name);
        try t.expectString("n1", user.note.?);
        try t.expectEqual(.red, user.choice);
    }

    {
        // null, dupe with explicit allocator
        var row = (try c.row("select 1::integer, true, 'teg', null::text, 'red'", .{})).?;
        const user = try row.to(User, .{ .allocator = t.allocator });
        row.deinit() catch {};

        defer t.allocator.free(user.name);
        try t.expectEqual(1, user.id);
        try t.expectEqual(true, user.active);
        try t.expectString("teg", user.name);
        try t.expectEqual(null, user.note);
        try t.expectEqual(.red, user.choice);
    }

    {
        // not null, dupe with explicit allocator
        var row = (try c.row("select 2::integer, false, 'ghanima', 'n1', 'red'", .{})).?;

        const user = try row.to(User, .{ .allocator = t.allocator });
        row.deinit() catch {};

        defer t.allocator.free(user.name);
        defer t.allocator.free(user.note.?);

        try t.expectEqual(2, user.id);
        try t.expectEqual(false, user.active);
        try t.expectString("ghanima", user.name);
        try t.expectString("n1", user.note.?);
        try t.expectEqual(.red, user.choice);
    }
}

test "Row.to: name no map" {
    const User = struct {
        id: i32 = 9876,
        active: bool,
        name: []const u8,
        note: ?[]const u8 = null,
    };

    var c = t.connect(.{});
    defer c.deinit();

    {
        // null, no dupe
        var row = (try c.rowOpts("select 1 as id, true as active, 'teg' as name, null as note", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .map = .name });
        try t.expectEqual(1, user.id);
        try t.expectEqual(true, user.active);
        try t.expectString("teg", user.name);
        try t.expectEqual(null, user.note);
    }

    {
        // default values are used if no colum
        // and extra columns are ignored
        var row = (try c.rowOpts("select 2 as id, false as active, 'ghanima' as name, 'x123' as other", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .map = .name });
        try t.expectEqual(2, user.id);
        try t.expectEqual(false, user.active);
        try t.expectString("ghanima", user.name);
        try t.expectEqual(null, user.note);
    }

    {
        // nullable fields are nulled if no column
        // and extra columns are ignored
        var row = (try c.rowOpts("select false as active, 'ghanima' as name, 'x123' as other", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .map = .name });
        try t.expectEqual(9876, user.id);
        try t.expectEqual(false, user.active);
        try t.expectString("ghanima", user.name);
        try t.expectEqual(null, user.note);
    }

    {
        // error on missing column with non-default value
        var row = (try c.rowOpts("select 1 as id", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        try t.expectError(error.FieldColumnMismatch, row.to(User, .{ .map = .name }));
    }

    {
        // not null, no dupe
        var row = (try c.rowOpts("select 2::integer as id, false as active, 'ghanima' as name, 'n1' as note", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .map = .name });
        try t.expectEqual(2, user.id);
        try t.expectEqual(false, user.active);
        try t.expectString("ghanima", user.name);
        try t.expectString("n1", user.note.?);
    }

    {
        // null, dupe with internal arena
        var row = (try c.rowOpts("select 1::integer as id, true as active, 'teg' as name, null::text as note", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .dupe = true, .map = .name });
        try t.expectEqual(1, user.id);
        try t.expectEqual(true, user.active);
        try t.expectString("teg", user.name);
        try t.expectEqual(null, user.note);
    }

    {
        // not null, dupe with internal arena
        var row = (try c.rowOpts("select 2::integer as id, false as active, 'ghanima' as name, 'n1' as note", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .dupe = true, .map = .name });
        try t.expectEqual(2, user.id);
        try t.expectEqual(false, user.active);
        try t.expectString("ghanima", user.name);
        try t.expectString("n1", user.note.?);
    }

    {
        // null, dupe with explicit allocator
        var row = (try c.rowOpts("select 1::integer as id, true as active, 'teg' as name, null::text as note", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .allocator = t.allocator, .map = .name });
        defer t.allocator.free(user.name);
        try t.expectEqual(1, user.id);
        try t.expectEqual(true, user.active);
        try t.expectString("teg", user.name);
        try t.expectEqual(null, user.note);
    }

    {
        // not null, dupe with explicit allocator
        var row = (try c.rowOpts("select 5::integer as id, false as active, 'ghanima' as name, 'n1' as note", .{}, .{ .column_names = true })).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{ .allocator = t.allocator, .map = .name });
        defer t.allocator.free(user.name);
        defer t.allocator.free(user.note.?);

        try t.expectEqual(5, user.id);
        try t.expectEqual(false, user.active);
        try t.expectString("ghanima", user.name);
        try t.expectString("n1", user.note.?);
    }
}

test "Result.Mapper" {
    var c = t.connect(.{});
    defer c.deinit();

    {
        // mapper with missing column and non-default field
        var result = try c.queryOpts("select 1", .{}, .{ .column_names = true });
        defer result.deinit();
        const mapper = result.mapper(struct { id: i32 }, .{});
        try t.expectError(error.FieldColumnMismatch, mapper.next());
        try result.drain();
    }

    // null, no dupe
    try expectResultMapper(&c, "select 1 as id, true as active, 'teg' as name, null as note", .{
        .id = 1,
        .active = true,
        .name = "teg",
        .note = null,
    }, .{});

    // default values are used if no colum
    // and extra columns are ignored
    try expectResultMapper(&c, "select 2 as id, false as active, 'ghanima' as name, 'x123' as other", .{
        .id = 2,
        .active = false,
        .name = "ghanima",
        .note = null,
    }, .{});

    // nullable fields are nulled if no column
    // and extra columns are ignored
    try expectResultMapper(&c, "select false as active, 'ghanima' as name, 'x123' as other", .{
        .id = 9876,
        .active = false,
        .name = "ghanima",
        .note = null,
    }, .{});

    // not null, no dupe
    try expectResultMapper(&c, "select 2::integer as id, false as active, 'ghanima' as name, 'n1' as note", .{
        .id = 2,
        .active = false,
        .name = "ghanima",
        .note = "n1",
    }, .{});

    // null, dupe with internal arena
    try expectResultMapper(&c, "select 1::integer as id, true as active, 'teg' as name, null::text as note", .{
        .id = 1,
        .active = true,
        .name = "teg",
        .note = null,
    }, .{ .dupe = true });

    // not null, dupe with internal arena
    try expectResultMapper(&c, "select 3::integer as id, false as active, 'ghanima' as name, 'n1' as note", .{
        .id = 3,
        .active = false,
        .name = "ghanima",
        .note = "n1",
    }, .{ .dupe = true });

    // null, dupe with explicit allocator
    try expectResultMapper(&c, "select 4::integer as id, true as active, 'teg' as name, null::text as note", .{
        .id = 4,
        .active = true,
        .name = "teg",
        .note = null,
    }, .{ .allocator = t.allocator });

    // not null, dupe with explicit allocator
    try expectResultMapper(&c, "select 5::integer as id, false as active, 'ghanima' as name, 'n1' as note", .{
        .id = 5,
        .active = false,
        .name = "ghanima",
        .note = "n1",
    }, .{ .allocator = t.allocator });
}

test "Row.to: iterator" {
    const User = struct {
        parents: Iterator(i32),
        tags: ?Iterator([]const u8),
    };

    defer t.reset();
    var c = t.connect(.{});
    defer c.deinit();

    {
        var row = (try c.row("select array[1, 99]::integer[], null", .{})).?;
        defer row.deinit() catch {};

        const user = try row.to(User, .{});
        try t.expectSlice(i32, &.{ 1, 99 }, try user.parents.alloc(t.arena.allocator()));
        try t.expectEqual(null, user.tags);
    }

    {
        var row = (try c.row("select array[0]::integer[], array['over', '9000']::text[]", .{})).?;
        const user = try row.to(User, .{ .allocator = t.allocator });
        row.deinit() catch {};

        defer user.parents.deinit(t.allocator);
        defer user.tags.?.deinit(t.allocator);

        try t.expectSlice(i32, &.{0}, try user.parents.alloc(t.arena.allocator()));
        try t.expectStringSlice(&.{ "over", "9000" }, try user.tags.?.alloc(t.arena.allocator()));
    }

    {
        // dupe with result arena
        var result = try c.query(
            \\ select array[0]::integer[], array['over']::text[]
            \\ union all
            \\ select array[1]::integer[], array['9000']::text[]
        , .{});

        const user1 = try (try result.next()).?.to(User, .{ .dupe = true });
        const user2 = try (try result.next()).?.to(User, .{ .dupe = true });
        try t.expectEqual(null, try result.next());
        defer result.deinit();

        try t.expectSlice(i32, &.{0}, try user1.parents.alloc(t.arena.allocator()));
        try t.expectStringSlice(&.{"over"}, try user1.tags.?.alloc(t.arena.allocator()));

        try t.expectSlice(i32, &.{1}, try user2.parents.alloc(t.arena.allocator()));
        try t.expectStringSlice(&.{"9000"}, try user2.tags.?.alloc(t.arena.allocator()));
    }

    {
        // dupe with explicit arena
        var result = try c.query(
            \\ select array[0]::integer[], array['over']::text[]
            \\ union all
            \\ select array[1]::integer[], array['9000']::text[]
        , .{});

        const user1 = try (try result.next()).?.to(User, .{ .allocator = t.allocator });
        const user2 = try (try result.next()).?.to(User, .{ .allocator = t.allocator });
        try t.expectEqual(null, try result.next());
        result.deinit();

        defer user1.tags.?.deinit(t.allocator);
        defer user1.parents.deinit(t.allocator);
        defer user2.tags.?.deinit(t.allocator);
        defer user2.parents.deinit(t.allocator);

        try t.expectSlice(i32, &.{0}, try user1.parents.alloc(t.arena.allocator()));
        try t.expectStringSlice(&.{"over"}, try user1.tags.?.alloc(t.arena.allocator()));

        try t.expectSlice(i32, &.{1}, try user2.parents.alloc(t.arena.allocator()));
        try t.expectStringSlice(&.{"9000"}, try user2.tags.?.alloc(t.arena.allocator()));
    }
}

fn expectResultMapper(conn: *Conn, sql: []const u8, expected: anytype, opts: Result.MapperOpts) !void {
    const User = struct {
        id: i32 = 9876,
        active: bool,
        name: []const u8,
        note: ?[]const u8 = null,
    };

    var result = try conn.queryOpts(sql, .{}, .{ .column_names = true });
    defer result.deinit();
    var mapper = result.mapper(User, opts);

    const user = (try mapper.next()) orelse unreachable;
    try t.expectEqual(expected.id, user.id);
    try t.expectEqual(expected.active, user.active);
    try t.expectString(expected.name, user.name);
    if (opts.allocator) |a| {
        a.free(user.name);
    }
    if (@TypeOf(expected.note) == @TypeOf(null)) {
        try t.expectEqual(null, user.note);
    } else {
        try t.expectString(expected.note, user.note.?);
        if (opts.allocator) |a| {
            a.free(user.note.?);
        }
    }

    try t.expectEqual(null, mapper.next());
}
