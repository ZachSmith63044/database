#include "dbone/columns/dataTypes.hpp"
#include "dbone/serialize.hpp"
#include <stdexcept>

// ================= BigIntType =================
BigIntType::BigIntType(int64_t v) : value_(v) {}

void BigIntType::to_bits(BitBuffer &buf) const {
    for (int i = 0; i < 64; i++) {
        bool bit = (value_ >> i) & 1;
        buf.putBit(bit);
    }
}

BigIntType BigIntType::from_bits(const std::vector<uint8_t> &payload, size_t& ref) {
    int64_t v = readI64(payload, ref);
    return BigIntType(v);
}

std::string BigIntType::default_value_str() const {
    return std::to_string(value_);
}

std::unique_ptr<BigIntType> BigIntType::parse(const std::string &s) {
    try {
        long long v = std::stoll(s);
        return std::make_unique<BigIntType>(v);
    } catch (...) {
        throw std::runtime_error("BigIntType::parse: invalid integer string '" + s + "'");
    }
}

// ================= CharType =================
CharType::CharType(std::string v, uint32_t length)
    : value_(std::move(v)), length_(length) {}

void CharType::to_bits(BitBuffer& buf) const {
    if (value_.size() != length_) {
        throw std::runtime_error("CharType::to_bits: value size != schema length");
    }
    for (char c : value_) {
        buf.putU8(static_cast<uint8_t>(c));
    }
}

CharType CharType::from_bits(const std::vector<uint8_t> &payload, size_t& ref, uint32_t length) {
    std::string s;
    s.reserve(length);
    for (uint32_t i = 0; i < length; i++) {
        char c = static_cast<char>(readU8(payload, ref));
        s.push_back(c);
    }
    return CharType(s, length);
}

std::string CharType::default_value_str() const {
    return "'" + value_ + "'";
}

std::unique_ptr<CharType> CharType::parse(const std::string &s, uint32_t length) {
    if (s.size() != length) {
        throw std::runtime_error("CharType::parse: string length mismatch (expected " +
                                 std::to_string(length) + ", got " + std::to_string(s.size()) + ")");
    }
    return std::make_unique<CharType>(s, length);
}
