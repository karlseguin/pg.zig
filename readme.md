# Native PostgreSQL driver for Zig

A native PostgresSQL driver / client for Zig.

## Example
```zig
var pool = try pg.Pool.init(allocator, .{
  .size = 5,
  .connect = .{
    .port = 5432,
    .host = "127.0.0.1",
  },
  .auth = .{
    .username = "postgres",
    .database = "postgres",
    .password = "root_pw",
    .timeout = 10_000,
  }
});
defer pool.deinit();

var conn = try pool.acquire();
defer conn.release();

const sql = "select id, name from users where power > $1";
var result = conn.query(sql, .{9000}) catch |err| switch (err) {
  error.PG => {
    std.debug.print("PG: {s}", {conn.err.?.message});
    return err;
  },
  else => return err,
}
defer result.deinit();

while (try result.next()) |row| {
  const id = row.get(i32, 0);
  // this is only valid until the next call to next(), deinit() or drain()
  const name = row.get([]u8, 1);
}
```

## Conn

### open(allocator: std.mem.Allocator, opts: Conn.ConnectOpts) !Conn
Opens a connection, or returns an error. Prefer creating connections through the pool. Connection options are:

* `host` - Defaults to `"127.0.0.1"`
* `port` - Defaults to `5432`
* `write_buffer` - Size of the write buffer, used when sending messages to the server. Will temporarily allocate more space as needed. If you're writing large SQL or have large parameters (e.g. long text values), making this larger might improve performance a little. Defaults to `2048`.
* `read_buffer` - Size of the read buffer, used when reading data from the server. Will temporarily allocate more space as needed. Given most apps are going to be reading rows of data, this can have large impact on performance. Defaults to `4096`.
* `result_state_size` - Each `Result` (retrieved via a call to `query`) carries metadata about the data (e.g. the type of each column). For results with less than or equal to `result_state_size` columns, a static `state` container is used. Queries with more columns require a dynamic allocation. Defaults to `32`. 

### deinit(conn: \*Conn) void
Closes the connection and releases its resources. This method should not be used when the connection comes from the pool.

### auth(opts: Conn.AuthOpts) !void
Authentications the request. Prefer creating connections through the pool. Auth options are:

* `username`: Defaults to `"postgres"`
* `password`: Defaults to `null`
* `database`: Defaults to `null`
* `timeout` : Defaults to `10_000` (milliseconds)


### release(conn: \*Conn) void
Releases the connection back to the pool. The pool might decide to close the connection and open a new one.

### exec(sql: []const u8, args: anytype) !?usize
Executes the query with arguments, returns the number of rows affected, or null. Should not be used with a query that returns rows.

### query(sql: []const u8, args: anytype) !Result
Executes the query with arguments, returns a result. `deinit`, and possibly `drain`, must be called on the returned `Result`.

### queryOpts(sql: []const u8, args: anytype, opts: Conn.QueryOpts) !Result
Same as `query` but takes options:

- `timeout` - This is not reliable and should probably not be used. Currently it simply puts a recv socket timeout. On timeout, the connection will likely no longer be valid (which the pool will detect and handle when the connection is released) and the underlying query will likely still execute. Defaults to `null`
- `column_names` - Whether or not the `result.column_names` should be populated. When true, this requires memory allocation (duping the column names). Default to `false`
- `allocator` - The allocator to use for any allocations needed when executing the query and reading the results. When `null` this will default to the connection's allocator. If you were executing a query in a web-request and each web-request had its own arena tied to the lifetime of the request, it might make sense to use that arena. Defaults to `null`.

### row(sql: []const u8, args: anytype) !?QueryRow
Executes the query with arguments, returns a single row. Returns an error if the query returns more than one row. Returns null if the query returns no row. `deinit` must be called on the returned `QueryRow`.

### rowOpts(sql: []const u8, args: anytype, opts: Conn.QueryOpts) !Result
Same as `row` but takes the same options as `queryOpts`

### begin() !void
Calls `_ = try execOpts("begin", .{}, .{})`

### commit() !void
Calls `_ = try execOpts("commit", .{}, .{})`

### rollback() !void
Calls `_ = try execOpts("rollback", .{}, .{})`

## Result
The `conn.query` and `conn.queryOpts` methods return a `pg.Result` which is used to read rows and values.

### Fields
* `number_of_columns: usize` - Number of columns in the result
* `column_names: [][]const u8` - Names of the column, empty unless the query was executed with the `column_names = true` option.

### deinit(result: \*Result) void
Releases resources associated with the result.

### drain(result: \*Result) !void
If you do not iterate through the result until `next` returns `null`, you must call `drain`. 

Why can't `deinit` handle this? If `deinit` also drained, you'd have to handle a possible error in `deinit` and you can't `try` in a defer. Thus, this is done to provide better ergonomics for the normal case - the normal case being where `next` is called until it returns `null`. In these cases, just `defer result.deinit()`.

### next(result: \*Result) !?Row
Iterates to the next row of the result, or returns null if there are no more rows.

### columnIndex(name: []const u8) ?usize
Returns the index of the column with the given name. This is only valid when the query is executed with the `column_names = true` option.

## Row
The `row` represents a single row from a result. Any non-primitive value that you get from the `row` are valid only until the next call to `next`, `deinit` or `drain`.

### Fields
Only advance usage will need access to the row fields:

* `oids: []i32` - The PG OID value for each column in the row. See `result.number_of_columns` for the length of this slice. Might be useful if you're trying to read a non-natively supported type.
* `values: []Value` - The underlying byte value for each column in the row.  See `result.number_of_columns` for the length of this slice. Might be useful if you're trying to read a non-natively supported type. Has two fields, `is_null: bool` and `data: []const u8`.

### get(comptime T: type, col: usize) T
Gets a single value from the row at the specified column index (0-based). **Type mapping is strict.** For example, you **cannot** use `i32` to read an `smallint` column.

For any supported type, you can use an optional instead. Therefore, if you use `row.get(i16, 0)` the return type is `i16`. If you use `row.get(?i16, 0)` the return type is `?i16`. If you use a non-optional type for a null value, you'll get a failed assertion in `Debug` and `ReleaseSafe`, and undefined behavior in `ReleaseFast`, `ReleaseSmall` or if you set `pg_assert = false`.

* `u8` - `char`
* `i16` - `smallint`
* `i32` - `int`
* `i64` - Depends on the underlying column type. A `timestamp(tz)` will be converted to microseconds since unix epoch. Otherwise, a `bigint`.
* `f32` - `float4`
* `f64` - Depends on the underlying column type. A `numeric` will be converted to an `f64`. Otherwise, a `float`.
* `bool` - `bool`
* `[]const u8` - Returns the raw underlying data. Can be used for any column type to get the PG-encoded value. For `text` and `bytea` columns, this will be the expected value. For `numeric`, this will be a text representation of the number. For `UUID` this will be a 16-byte slice (use `pg.uuidToHex [36]u8` if you want a hex-encoded UUID). For `JSON` and `JSONB` this will be the serialized JSON value.
* `[]u8` - Same as []const u8 but returns a mutable value.
* `pg.Numeric` - See numeric section
* `pg.Cidr` - See CIDR/INET section

### getCol(comptime T: type, column_name: []const u8) T
Same as `get` but uses the column name rather than its position. Only valid when the `column_names = true` option is passed to `queryOpts`.

This relies on calling `result.columnIndex` which iterates through `result.column_names` fields. In some cases, this is more efficient than `StringHashMap` lookup, in others, it is worse. For performance-sensitive code, prefer using `get`, or cache the column index in a local variables outside of the `next()` loop:

```zig
const id_idx = result.columnIndex("id").?
while (try result.next()) |row| {
  // row.get(i32, id_idx)
}
```

### iterator(comptime T: type, col: usize) Iterator(T)
Used for reading a PostgreSQL array. Optional/null support is the same as `get`.

* `u8` - `char[]`
* `i16` - `smallint[]`
* `i32` - `int[]`
* `i64` - `bigint[]` or `timestamp(tz)[]` (see `get`)
* `f32` - `float4`
* `f64` - `float8`
* `bool` - `bool[]`
* `[]const u8` - More strict than `get([]u8)`). Supports: `text[]`, `char(n)[]`, `bytea[]`, `uuid[]`, `json[]` and `jsonb[]`
* `[]u8` - Same as `[]const u8` but returns mutable value.
* `pg.Numeric` - See numeric section
* `pg.Cidr` - See CIDR/INET section

### iteratorCol(comptime T: typee, column_name: []const u8) Iterator(T)
See `getCol`.

## QueryRow
A `QueryRow` is returned from a call to `conn.row` or `conn.rowOpts` and wraps both a `Result` and a `Row.` It exposes the same methods as `Row` as well as `deinit`, which must be called once the `QueryRow` is no longer needed.

## Iterator(T)
The iterator returned from `row.iterator(T, col)` can be iterated using the `next() ?T` call:

```zig
var names = row.iterator([]u8, 0);
while (names.next()) |name| {
  ...
}
```

### Fields
* `len` - the number of values in the iterator

### alloc(it: Iterator(T), allocator: std.mem.Allocator) ![]T
Allocates a slice and populates it with all values.

### fill(it: Iterator(T), into: []T) void
Fill `into` with values of the iterator. `into` can be smaller than `it.len`, in which case only `into.len` values will be filled. This can be a bit faster than calling `next()` multiple times.

## Important Notice 1 - Bind vs Read
When you read a value, such as `row.get(i32, 0)`, the library assumes you know what you're doing and that column 0 really is a non-null 32-bit integer. `row.get` doesn't return an error union. There are some assertions, but these are disabled in ReleaseFast and ReleaseSmall. You can also disable these assertions in Debug/ReleaseSafe by placing `pub const pg_assert = false;` in your root, (e.g. `main.zig`):

```zig
const std = @import("std");
...

pub const pg_assert = false;

pub fm main() !void {
  ...
}
```

Conversely, when binding a value to an SQL parameter, the library is a little more generous. For example, an `u64` will bind to an `i32` provided the value is within range.

This is particularly relevant for types which are expressed as `[]u8`. For example a UUID can be a raw binary `[16]u8` or a hex-encoded `[36]u8`. Where possible (e.g. UUID, MacAddr, MacAddr8), the library will support binding either the raw binary data or text-representation. When reading, the raw binary value is always returned.

## Important Notice 2 - Invalid Connections
Strongly consider using `pg.Pool` rather than using `pg.Conn` directly. The pool will attempt to reconnect disconnected connections or connections which are in an invalid state. Until more real world testing is done, you should assume that connections will get into invalid states.

## Important Notice 3 - Errors
Zig errorsets do not support arbitrary payloads. This is problematic in a database driver where most applications probably care about the details of an error. The library takes a simple approach. If `error.PG` is returned, `conn.err` should be set and will contains a PG error object:

```zig
_ = conn.exec("drop table x", .{}) catch |err| {
  if (err == error.PG) {
    if (conn.err) |pge| {
      std.log.err("PG {s}\n", .{pge.message});
    }
  }
  return err;
};
```

In the above snippet, it's possible to skip the `if (err == error.PG)` check, but in that case `conn.err` could be set from some previous command (`conn.err` is always reset when acquired from the pool).

If `error.PG` is returned from a non-connection object, like a query result, the associated connection will have its `conn.err` set. In other words, `conn.err` is the only thing you ever have to check.

A PG error always exposes the following fields:
* `code: []const u8` - https://www.postgresql.org/docs/current/errcodes-appendix.html
* `message: []const u8`
* `severity: []const u8`

And optionally (depending on the error and the version of the server):
* `column: ?[]const u8 = null`
* `constraint: ?[]const u8 = null`
* `data_type_name: ?[]const u8 = null`
* `detail: ?[]const u8 = null`
* `file: ?[]const u8 = null`
* `hint: ?[]const u8 = null`
* `internal_position: ?[]const u8 = null`
* `internal_query: ?[]const u8 = null`
* `line: ?[]const u8 = null`
* `position: ?[]const u8 = null`
* `routine: ?[]const u8 = null`
* `schema: ?[]const u8 = null`
* `severity2: ?[]const u8 = null`
* `table: ?[]const u8 = null`
* `where: ?[]const u8 = null`

The `isUnique() bool` method can be called on the error to determine whether or not the error was a unique violation (i.e. error code `23505`).

## Type Support
All implementations have to deal with things like: how to support unsigned integers, given that PostgreSQL only has signed integers. Or, how to support UUIDs when the language has no UUID type. This section documents the exact behavior.

### Arrays
Multi-dimensional arrays aren't supported. The array lower bound is always 0 (or 1 in PG)

### text, bool, bytea, char, char(n), custom enums
No surprises, arrays supported. 

When reading a `char[]`, it's tempting to use `row.get([]u8, 0)`, but this is incorrect. A `char[]` is an array, and thus `row.iterator(u8, 0`) must be used.

### smallint, int, bigint
When binding an integer, the library will coerce the Zig value to the parameter type, as long as it fits. Thus, a `u64` can be bound to a `smallint`, if the value fits, else an error will be returned.

Array binding is strict. For example, an `[]i16` must be used for a `smallint[]`parameter. The only exception is that the unsigned variant, e.g. `[]u16` can be used provided all values fit.

When reading a column, you must use the correct type. 

### Floats
When binding, `@floatCast` is used based on the SQL parameter type. Array binding is strict. When reading a value, you must use the correct type. 

### Numeric
Until standard support comes to Zig (either in the stdlib or a de facto standard library), numeric support is half-baked. When binding a value to a parameter, you can use a f32, f64, comptime_float or string. The same applies to binding to a numeric array.

You can `get(pg.Numeric, $COL)` to return a `pg.Numeric`. The `pg.Numeric` type only has 2 useful methods: `toFloat` and `toString`. You can also use `num.estimatedStringLen` to get the max size of the string representation:

```zig
const numeric = row.get(pg.Numeric, 0);
var buf = allocator.alloc(u8, numeric.estimatedStringLen());
defer allocator.free(buf)
const str = numeric.toString(&buf);
```

Using `row.get(f64, 0)` on a numeric is the same as `row.get(pg.Numeric, 0).toFloat()`.

You should consider simply casting the numeric to `::double` or `::text` within SQL in order to rely on PostgreSQL's own robust numeric to float/text conversion.

However, `pg.Numeric` has fields for the underlying wire-format of the numeric value. So if you require precision and the text representation isn't sufficient, you can parse the fields directly. `types/numeric.zig` is relatively well documented and tries to explain the fields. Note that any non-primitive fields, e.g. the  `digits: []u8`, is only valid until the next call to `result.next`, `result.deinit`, `result.drain` or `row.deinit`.

### UUID
When a `[]u8` is bound to a UUID column, it must either be a 16-byte slice, or a valid 36-byte hex-encoded UUID. Arrays behave the same.

When reading a `uuid` column with `[]u8` a 16-byte slice will be returned. Use the `pg.uuidToHex() ![36]u8` helper if you need it hex-encoded.

### INET/CIDR
You can bind a string value to a `cidr`, `inet`, `cidr[]` or `inet[]` parameter.

When reading a value, via `row.get` or `row.iterator` you should use `pg.Cidr`. It exposes 3 fields:

* `address: []u8` - Will be a 4 or 16 byte slice depending on the family
* `family: Family` - An enum, either `Family.v4` of `Family.v6`
* `netmask: u8` - The network mask

### MacAddr/MacAddr8
You can bind a `[]u8` to either a `macaddr` or a `macaddr8`. These can be either binary representation (6-bytes for `macaddr` or 8 bytes for `macaddr8`) or a text-representation supported by PostgreSQL. This works, like UUID, because there's no ambiguity in the length. The same applied for array variants - it's even possible to mix and match formats within the array.

When reading a value, via `row.get` or `row.iterator` using `[]u8`, the binary representation is always returned.

### Timestamp(tz)
When you bind an `i64` to a timestamp(tz) parameter, the value is assumed to be the number of microseconds since unix epoch (e.g. `std.time.microTimestamp()`). Array binding works the same. You can also bind a string, which will pass the string as-is and depend on PostgreSQL to do the conversion. This is true for arrays as well.

When reading a `timestamp` column with `i64`, the number of microseconds since unix epoch will be returned

### JSON and JSONB
When binding a value to a JSON or JSONB parameter, you can either supply a serialized value (i.e. `[]u8`) or a struct which will be serialized using `std.json.stringify`.

When binding to an array of JSON or JSONB, automatic serialization is not support and thus an array of serialized values must be provided.

When reading a `JSON` or `JSONB` column with `[]u8`, the serialized JSON will be returned.

## Listen / Notify
You can create a `pg.Listener` either from an existing `Pool` or directly. When using the pool, a new connection/session is created. It *does not* use a connection from the pool:

```zig
var listener = try pool.newListener();
defer listener.deinit();

// listen to 1 or more channels
try listener.listen("chan_1");
try listener.listen("chan_2");

// .next() blocks until there's a notification or an error
while (listener.next()) |notification| {
  std.debug.print("Channel: {s}\nPayload: {s}", .{notification.channel, notification.payload});
}

// Yes, this API is annoying. I hope Zig adds error payloads soon.

// can only be here if an error occurred.
switch (listener.err.?) {
  error.PG => std.debug.print("{s}\n", .{listener.conn.err.?.message}),
  else => |err| std.debug.print("{s}\n", .{@errorName(err)}),
}
```

Creating a new Listener directly is a lot like creating a new connection. See [Conn.open](#openallocator-stdmemallocator-opts-connconnectopts-conn) and [Conn.auth](#authopts-connauthopts-void).

```zig
// see the Conn.ConnectOpts
var l = try pg.Listener.open(allocator, .{});
defer l.deinit();

try l.auth(.{})

// same as above
try listener.listen("chan_1");
...
```
