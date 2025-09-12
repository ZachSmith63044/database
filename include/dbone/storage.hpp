#pragma once
#include <cstdint>
#include <cstdio>
#include <string>

namespace dbone::storage {

// Cross-platform 64-bit file seek
int seek64(FILE *f, long long off, int whence);

// Write raw bytes at absolute offset
bool write_at64(FILE *f, uint64_t offset, const void *data, size_t len);

// Extend file to exact size (writes trailing zero)
bool extend_file(FILE *f, uint64_t file_size, std::string *err);

} // namespace dbone::storage
