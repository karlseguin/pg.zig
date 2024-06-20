const std = @import("std");
const builtin = @import("builtin");

const pg = @import("pg");

pub const log = std.log.scoped(.example);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = if (builtin.mode == .Debug) gpa.allocator() else std.heap.c_allocator;


    var pool = pg.Pool.init(allocator, .{
        .size = 5,
        .connect = .{
            .port = 5432,
            .host = "127.0.0.1",
        },
        .auth = .{
            .username = "postgres",
            .database = "postgres",
            .timeout = 10_000,
        }
    }) catch |err| {
        log.err("Failed to connect: {}", .{err});
        std.posix.exit(1);
    };
    defer pool.deinit();



    // One-off commands can be executed directly using the pool using the
    // exec, execOpts, query, queryOpts, row, rowOpts functions. But, due to
    // Zig's lack of error payloads, if these fail, you won't be able to retrieve
    // a more detailed error
    _ = try pool.exec("drop table if exists pg_example_users", .{});


    {
        // Alternative, you can acquire/release connections from the pool
        // Ideal if you want to execute multiple statements and also exposes
        // a more detailed error.
        var conn = try pool.acquire();
        defer conn.release();

        // exec returns the # of rows affected for insert/select/delete
        _ = conn.exec("create table pg_example_users (id integer, name text)", .{}) catch |err| {
            if (conn.err) |pg_err| {
                log.err("create failure: {s}", .{pg_err.message});
            }
            return err;
        };
    }


    // Of course, exec can take parameters:
    _ = try pool.exec(
        "insert into pg_example_users (id, name) values ($1, $2), ($3, $4)",
        .{1, "Leto", 2, "Ghanima"}
    );


    {
        // we can fetch a single row:
        var conn = try pool.acquire();
        defer conn.release();

        var row = (try conn.row("select name from pg_example_users where id = $1", .{1})) orelse unreachable;
        // having to deal with row.deinit() is an unfortunate consequence of the
        // conn.row and pool.row API. Sorry!
        defer row.deinit() catch {};

        // name will become invalid after row.deinit() is called. dupe it if you
        // need it to live longer.
        const name = row.get([]const u8, 0);
        log.info("User 1: {s}", .{name});
    }


    {
        // or we can fetch multiple rows:
        // we can fetch a single row:
        var conn = try pool.acquire();
        defer conn.release();

        var result = try conn.query("select * from pg_example_users order by id", .{});
        defer result.deinit();

        while (try result.next()) |row| {
            const id = row.get(i32, 0);
            // string values are only valid until the next call to next()
            // dupe the value if needed
            const name = row.get([]const u8, 1);
            log.info("User {d}: {s}", .{id, name});
        }
    }
}
