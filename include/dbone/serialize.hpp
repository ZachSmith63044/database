#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <stdexcept>

inline uint8_t readU8(const std::vector<uint8_t>& buf, size_t& off) {
    if (off + 1 > buf.size())
        throw std::runtime_error("readU8: out of bounds at off=" + std::to_string(off));
    return buf[off++];
}

inline uint16_t readU16(const std::vector<uint8_t>& buf, size_t& off) {
    if (off + 2 > buf.size())
        throw std::runtime_error("readU16: out of bounds at off=" + std::to_string(off));
    uint16_t v = buf[off] | (buf[off+1] << 8);
    off += 2;
    return v;
}

inline uint32_t readU32(const std::vector<uint8_t>& buf, size_t& off) {
    if (off + 4 > buf.size())
        throw std::runtime_error("readU32: out of bounds at off=" + std::to_string(off));
    uint32_t v = buf[off] | (buf[off+1] << 8) | (buf[off+2] << 16) | (buf[off+3] << 24);
    off += 4;
    return v;
}

inline int64_t readI64(const std::vector<uint8_t>& buf, size_t& off) {
    if (off + 8 > buf.size())
        throw std::runtime_error("readI64: out of bounds at off=" + std::to_string(off));

    uint64_t v =  static_cast<uint64_t>(buf[off]) |
                 (static_cast<uint64_t>(buf[off+1]) << 8) |
                 (static_cast<uint64_t>(buf[off+2]) << 16) |
                 (static_cast<uint64_t>(buf[off+3]) << 24) |
                 (static_cast<uint64_t>(buf[off+4]) << 32) |
                 (static_cast<uint64_t>(buf[off+5]) << 40) |
                 (static_cast<uint64_t>(buf[off+6]) << 48) |
                 (static_cast<uint64_t>(buf[off+7]) << 56);

    off += 8;
    return static_cast<int64_t>(v);
}

inline std::string readString(const std::vector<uint8_t>& buf, size_t& off) {
    uint16_t len = readU16(buf, off);
    if (off + len > buf.size())
        throw std::runtime_error("readString: out of bounds, len=" + std::to_string(len));
    std::string s(reinterpret_cast<const char*>(&buf[off]), len);
    off += len;
    return s;
}
