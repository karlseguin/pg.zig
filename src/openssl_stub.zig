// Used as the `openssl` import when TLS support is not enabled at build
// time. All openssl-using code paths are gated behind `lib.has_openssl`,
// so nothing in this module is ever referenced.
