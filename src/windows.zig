const std = @import("std");

pub const std_windows = std.os.windows;
const Win32Error = std_windows.Win32Error;
const UnexpectedError = std.posix.UnexpectedError;

pub const CloseHandle = std_windows.CloseHandle;