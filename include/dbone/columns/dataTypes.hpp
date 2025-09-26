#pragma once
#include <string>
#include <cstdint>
#include <memory>
#include "dbone/bitbuffer.hpp"
#include <ostream>

class DataType {
public:
    virtual ~DataType() = default;

    // --- Serialization ---
    virtual void to_bits(BitBuffer &buf) const = 0;
    virtual std::string default_value_str() const = 0;

    // --- Virtual comparators ---
    virtual bool equals(const DataType& other) const = 0;
    virtual bool less(const DataType& other) const = 0;

    // --- Clone support ---
    virtual std::unique_ptr<DataType> clone() const = 0;

    // --- Type name for debugging/logging ---
    virtual std::string type_name() const = 0;

    // Convenience wrappers
    bool operator==(const DataType& other) const { return equals(other); }
    bool operator!=(const DataType& other) const { return !equals(other); }
    bool operator<(const DataType& other)  const { return less(other); }
    bool operator>(const DataType& other)  const { return other.less(*this); }
    bool operator<=(const DataType& other) const { return !other.less(*this); }
    bool operator>=(const DataType& other) const { return !less(other); }
};

// ---------------- BigIntType ----------------
class BigIntType : public DataType {
public:
    explicit BigIntType(int64_t v);

    void to_bits(BitBuffer &buf) const override;
    static BigIntType from_bits(const std::vector<uint8_t> &payload, size_t& ref);

    std::string default_value_str() const override;
    static std::unique_ptr<BigIntType> parse(const std::string &s);

    int64_t value() const { return value_; }

    // Implement virtual comparison
    bool equals(const DataType& other) const override;
    bool less(const DataType& other) const override;

    // --- Clone ---
    std::unique_ptr<DataType> clone() const override {
        return std::make_unique<BigIntType>(*this);
    }

    // --- Type name ---
    std::string type_name() const override { return "BigIntType"; }

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
    static std::unique_ptr<CharType> parse(const std::string &s, uint32_t length);

    const std::string& value() const { return value_; }

    // Implement virtual comparison
    bool equals(const DataType& other) const override;
    bool less(const DataType& other) const override;

    // --- Clone ---
    std::unique_ptr<DataType> clone() const override {
        return std::make_unique<CharType>(*this);
    }

    // --- Type name ---
    std::string type_name() const override { return "CharType"; }

private:
    std::string value_;
    uint32_t length_;
};

// ---------------- VarCharType ----------------
class VarCharType : public DataType {
public:
    VarCharType(std::string v, uint32_t max_length);

    void to_bits(BitBuffer &buf) const override;
    static VarCharType from_bits(const std::vector<uint8_t> &payload, size_t& ref, uint32_t max_length);

    std::string default_value_str() const override;
    static std::unique_ptr<VarCharType> parse(const std::string &s, uint32_t max_length);

    const std::string& value() const { return value_; }

    // Implement virtual comparison
    bool equals(const DataType& other) const override;
    bool less(const DataType& other) const override;

    // --- Clone ---
    std::unique_ptr<DataType> clone() const override {
        return std::make_unique<VarCharType>(*this);
    }

    // --- Type name ---
    std::string type_name() const override { return "VarCharType"; }

private:
    std::string value_;
    uint32_t max_length_;
};
