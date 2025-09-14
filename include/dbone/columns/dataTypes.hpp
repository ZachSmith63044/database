#pragma once
#include <string>
#include <cstdint>
#include <memory>
#include "dbone/bitbuffer.hpp"

class DataType {
public:
    virtual ~DataType() = default;
    virtual void to_bits(BitBuffer &buf) const = 0;
    virtual std::string default_value_str() const = 0;
};

// ---------------- BigIntType ----------------
class BigIntType : public DataType {
public:
    explicit BigIntType(int64_t v);

    void to_bits(BitBuffer &buf) const override;
    static BigIntType from_bits(const std::vector<uint8_t> &payload, size_t& ref);

    std::string default_value_str() const override;

    // NEW: parse from string
    static std::unique_ptr<BigIntType> parse(const std::string &s);

private:
    int64_t value_;
};

// ---------------- CharType ----------------
class CharType : public DataType {
public:
    CharType(std::string v, uint32_t length);

    void to_bits(BitBuffer &buf) const override;
    static CharType from_bits(const std::vector<uint8_t> &payload, size_t& ref, uint32_t length);

    std::string default_value_str() const override;

    // NEW: parse from string
    static std::unique_ptr<CharType> parse(const std::string &s, uint32_t length);

private:
    std::string value_;
    uint32_t length_;
};
