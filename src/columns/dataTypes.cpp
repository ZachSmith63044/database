#include "dbone/columns/dataTypes.hpp"
#include <iostream>
#include <stdexcept>

// ---- BigIntType ----
void BigIntType::to_bits(BitBuffer &buf) const {
    for (int i = 0; i < 64; i++) {
        bool bit = (value_ >> i) & 1;
        buf.putBit(bit);
    }
}

BigIntType BigIntType::from_bits(BitBuffer &buf) {
    int64_t v = 0;
    for (int i = 0; i < 64; i++) {
        bool bit = buf.getBit();  // must add getBit() to BitBuffer
        if (bit) {
            v |= (1ull << i);
        }
    }
    return BigIntType(v);
}

std::string BigIntType::default_value_str() const {
    return std::to_string(value_);
}

// ---- CharType ----
void CharType::to_bits(BitBuffer &buf) const {
    if (value_.size() != length_) {
        throw std::runtime_error("CharType::to_bits: value size != schema length");
    }
    for (char c : value_) {
        buf.putU8(static_cast<uint8_t>(c));
    }
}

CharType CharType::from_bits(BitBuffer &buf, uint32_t length) {
    std::string s;
    s.reserve(length);
    for (uint32_t i = 0; i < length; i++) {
        char c = static_cast<char>(buf.getU8()); // must add getU8() to BitBuffer
        s.push_back(c);
    }
    return CharType(s, length);
}

std::string CharType::default_value_str() const {
    return "'" + value_ + "'";
}
