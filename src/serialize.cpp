#include "dbone/serialize.hpp"

namespace dbone::serialize {

// --- Readers ---
uint8_t readU8(const std::vector<uint8_t>& buf, size_t& offset) {
    return buf[offset++];
}

uint16_t readU16(const std::vector<uint8_t>& buf, size_t& offset) {
    uint16_t v = static_cast<uint16_t>(buf[offset])
               | (static_cast<uint16_t>(buf[offset+1]) << 8);
    offset += 2;
    return v;
}

uint32_t readU32(const std::vector<uint8_t>& buf, size_t& offset) {
    uint32_t v = static_cast<uint32_t>(buf[offset])
               | (static_cast<uint32_t>(buf[offset+1]) << 8)
               | (static_cast<uint32_t>(buf[offset+2]) << 16)
               | (static_cast<uint32_t>(buf[offset+3]) << 24);
    offset += 4;
    return v;
}

std::string readString(const std::vector<uint8_t>& buf, size_t& offset) {
    uint16_t len = readU16(buf, offset);
    std::string s(buf.begin() + offset, buf.begin() + offset + len);
    offset += len;
    return s;
}

// --- Writers ---
void writeU8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

void writeU16(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
}

void writeU32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void writeString(std::vector<uint8_t>& buf, const std::string& s) {
    writeU16(buf, static_cast<uint16_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

} // namespace dbone::serialize
