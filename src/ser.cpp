#include "dbone/ser.hpp"

namespace dbone::ser {

void put_u32(FILE* f, uint32_t v) {
    unsigned char b[4] = {
        static_cast<unsigned char>( v        & 0xFF),
        static_cast<unsigned char>((v >> 8)  & 0xFF),
        static_cast<unsigned char>((v >> 16) & 0xFF),
        static_cast<unsigned char>((v >> 24) & 0xFF),
    };
    std::fwrite(b, 1, 4, f);
}

void put_u16(FILE* f, uint16_t v) {
    unsigned char b[2] = {
        static_cast<unsigned char>( v       & 0xFF),
        static_cast<unsigned char>((v >> 8) & 0xFF),
    };
    std::fwrite(b, 1, 2, f);
}

void put_u8(FILE* f, uint8_t v) {
    std::fwrite(&v, 1, 1, f);
}

void put_bytes(FILE* f, const std::string& s) {
    std::fwrite(s.data(), 1, s.size(), f);
}

} // namespace dbone::ser
