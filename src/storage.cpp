#include "dbone/storage.hpp"
#include <cerrno>

// --------- 64-bit seeking wrappers ----------
#if defined(_WIN32)
#include <io.h>
#include <fcntl.h>
int dbone::storage::seek64(FILE *f, long long off, int whence) {
    return _fseeki64(f, off, whence);
}
#else
#include <unistd.h>
int dbone::storage::seek64(FILE *f, long long off, int whence) {
    return fseeko(f, off, whence);
}
#endif

bool dbone::storage::write_at64(FILE *f, uint64_t offset, const void *data, size_t len) {
    if (!f) return false;
    if (seek64(f, static_cast<long long>(offset), SEEK_SET) != 0) {
        return false;
    }
    return std::fwrite(data, 1, len, f) == len;
}

bool dbone::storage::extend_file(FILE *f, uint64_t file_size, std::string *err) {
    if (!f) return false;
    if (seek64(f, static_cast<long long>(file_size - 1), SEEK_SET) != 0 ||
        std::fputc(0, f) == EOF) {
        if (err) *err = "failed to extend file";
        return false;
    }
    return true;
}
