#include "dbone/columns/dataTypes.hpp"
#include "dbone/serialize.hpp"
#include <stdexcept>

// ================= BigIntType =================
BigIntType::BigIntType(int64_t v) : value_(v) {}

void BigIntType::to_bits(BitBuffer &buf) const
{
    for (int i = 0; i < 64; i++)
    {
        bool bit = (value_ >> i) & 1;
        buf.putBit(bit);
    }
}

BigIntType BigIntType::from_bits(const std::vector<uint8_t> &payload, size_t &ref)
{
    int64_t v = readI64(payload, ref);
    return BigIntType(v);
}

std::string BigIntType::default_value_str() const
{
    return std::to_string(value_);
}

std::unique_ptr<BigIntType> BigIntType::parse(const std::string &s)
{
    try
    {
        long long v = std::stoll(s);
        return std::make_unique<BigIntType>(v);
    }
    catch (...)
    {
        throw std::runtime_error("BigIntType::parse: invalid integer string '" + s + "'");
    }
}

bool BigIntType::equals(const DataType &other) const
{
    if (auto p = dynamic_cast<const BigIntType *>(&other))
    {
        return value_ == p->value_;
    }
    throw std::runtime_error("Type mismatch in BigIntType::equals");
}

bool BigIntType::less(const DataType &other) const
{
    if (auto p = dynamic_cast<const BigIntType *>(&other))
    {
        return value_ < p->value_;
    }
    throw std::runtime_error("Type mismatch in BigIntType::less");
}

// ================= CharType =================
CharType::CharType(std::string v, uint32_t length)
    : value_(std::move(v)), length_(length) {}

void CharType::to_bits(BitBuffer &buf) const
{
    if (value_.size() != length_)
    {
        throw std::runtime_error("CharType::to_bits: value size != schema length");
    }
    for (char c : value_)
    {
        buf.putU8(static_cast<uint8_t>(c));
    }
}

CharType CharType::from_bits(const std::vector<uint8_t> &payload, size_t &ref, uint32_t length)
{
    std::string s;
    s.reserve(length);
    for (uint32_t i = 0; i < length; i++)
    {
        char c = static_cast<char>(readU8(payload, ref));
        s.push_back(c);
    }
    return CharType(s, length);
}

std::string CharType::default_value_str() const
{
    return "'" + value_ + "'";
}

std::unique_ptr<CharType> CharType::parse(const std::string &s, uint32_t length)
{
    if (s.size() != length)
    {
        throw std::runtime_error("CharType::parse: string length mismatch (expected " +
                                 std::to_string(length) + ", got " + std::to_string(s.size()) + ")");
    }
    return std::make_unique<CharType>(s, length);
}

bool CharType::equals(const DataType &other) const
{
    if (auto p = dynamic_cast<const CharType *>(&other))
    {
        return value_ == p->value_;
    }
    throw std::runtime_error("Type mismatch in CharType::equals");
}

bool CharType::less(const DataType &other) const
{
    if (auto p = dynamic_cast<const CharType *>(&other))
    {
        return value_ < p->value_;
    }
    throw std::runtime_error("Type mismatch in CharType::less");
}

// ================= VarCharType =================
VarCharType::VarCharType(std::string v, uint32_t max_length)
    : value_(std::move(v)), max_length_(max_length) {}

void VarCharType::to_bits(BitBuffer &buf) const
{
    if (value_.size() > max_length_)
    {
        throw std::runtime_error("VarCharType::to_bits: value size > schema max length");
    }

    // How many bytes do we need to store the length?
    int bits = static_cast<int>(std::ceil(std::log2(max_length_ + 1)));
    int len_bytes = (bits + 7) / 8; // round up to full bytes

    // std::cout << "BYTES NEEDED: " << len_bytes << std::endl;

    // Write length using len_bytes bytes (big-endian here, adjust if you want little-endian)
    size_t length = value_.size();
    for (int i = len_bytes - 1; i >= 0; --i)
    {
        uint8_t b = static_cast<uint8_t>((length >> (i * 8)) & 0xFF);
        buf.putU8(b);
    }

    // Write the actual characters
    for (char c : value_)
    {
        buf.putU8(static_cast<uint8_t>(c));
    }
}

VarCharType VarCharType::from_bits(const std::vector<uint8_t> &payload, size_t &ref, uint32_t max_length)
{
    // Determine how many bytes the length is stored in
    int bits = static_cast<int>(std::ceil(std::log2(max_length + 1)));
    int len_bytes = (bits + 7) / 8;

    // Read length
    uint32_t length = 0;
    for (int i = 0; i < len_bytes; i++)
    {
        length = (length << 8) | readU8(payload, ref);
    }

    if (length > max_length)
    {
        throw std::runtime_error("VarCharType::from_bits: length > max_length");
    }

    // Read characters
    std::string s;
    s.reserve(length);
    for (uint32_t i = 0; i < length; i++)
    {
        char c = static_cast<char>(readU8(payload, ref));
        s.push_back(c);
    }

    return VarCharType(s, max_length);
}

std::string VarCharType::default_value_str() const
{
    return "'" + value_ + "'";
}

std::unique_ptr<VarCharType> VarCharType::parse(const std::string &s, uint32_t max_length)
{
    if (s.size() > max_length)
    {
        throw std::runtime_error("VarCharType::parse: string length exceeds max_length (max " +
                                 std::to_string(max_length) + ", got " + std::to_string(s.size()) + ")");
    }
    return std::make_unique<VarCharType>(s, max_length);
}

bool VarCharType::equals(const DataType &other) const
{
    if (auto p = dynamic_cast<const VarCharType *>(&other))
    {
        return value_ == p->value_;
    }
    throw std::runtime_error("Type mismatch in CharType::equals");
}

bool VarCharType::less(const DataType &other) const
{
    if (auto p = dynamic_cast<const VarCharType *>(&other))
    {
        return value_ < p->value_;
    }
    throw std::runtime_error("Type mismatch in CharType::less");
}