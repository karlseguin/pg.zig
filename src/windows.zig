const std = @import("std");
pub const ws2_32 = @import("ws2_32.zig");

pub const std_windows = std.os.windows;
const Win32Error = std_windows.Win32Error;
const UnexpectedError = std.posix.UnexpectedError;

pub const CloseHandle = std_windows.CloseHandle;

pub fn unexpectedWSAError(err: ws2_32.WinsockError) UnexpectedError {
    return std_windows.unexpectedError(@as(Win32Error, @enumFromInt(@intFromEnum(err))));
}

