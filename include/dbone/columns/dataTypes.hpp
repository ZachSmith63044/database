#pragma once
#include <string>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include "dbone/bitbuffer.hpp"  // your BitBuffer class

// Base class for all data types
class DataType {
public:
    virtual ~DataType() = default;

    // Write value to buffer (bit-level packing)
    virtual void to_bits(BitBuffer &buf) const = 0;

    // Return default as human-readable string
    virtual std::string default_value_str() const = 0;
};

// -------- BigInt (64-bit integer) --------
class BigIntType : public DataType {
public:
    explicit BigIntType(int64_t value = 0) : value_(value) {}

    void to_bits(BitBuffer &buf) const override;
    std::string default_value_str() const override;

    static BigIntType from_bits(BitBuffer &buf);

    int64_t value() const { return value_; }

private:
    int64_t value_;
};

// -------- Fixed CHAR(N) --------
class CharType : public DataType {
public:
    CharType(std::string value = "", uint32_t length = 1)
        : value_(std::move(value)), length_(length) {
        if (value_.size() != length_) {
            throw std::runtime_error(
                "CharType value length mismatch with schema length");
        }
    }

    void to_bits(BitBuffer &buf) const override;
    std::string default_value_str() const override;

    static CharType from_bits(BitBuffer &buf, uint32_t length);

    std::string value() const { return value_; }
    uint32_t length() const { return length_; }

private:
    std::string value_;
    uint32_t length_;
};
