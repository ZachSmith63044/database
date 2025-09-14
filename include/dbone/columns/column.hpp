#pragma once
#include <string>
#include <memory>
#include "dbone/bitbuffer.hpp"
#include "dbone/columns/dataTypes.hpp"

enum class ColumnType : uint8_t {
    BIGINT = 1,
    CHAR   = 2,
};

class Column {
public:
    Column(std::string name,
           bool nullable,
           bool primaryKey,
           bool unique,
           std::unique_ptr<DataType> defaultVal);

    virtual ~Column() = default;

    // --- Serialization of schema ---
    virtual void to_bits(BitBuffer &buf) const = 0;

    // --- Parse from SQL string into a DataType instance ---
    virtual std::unique_ptr<DataType> parse(const std::string &raw) const = 0;

    // --- Deserialize from buffer into a DataType instance ---
    virtual std::unique_ptr<DataType> from_bits(const std::vector<uint8_t> &payload, size_t &ref) const = 0;

    // --- Accessors ---
    virtual ColumnType type() const = 0;

    virtual std::string default_value_str() const {
        return defaultVal_ ? defaultVal_->default_value_str() : "NULL";
    }

    const std::string& name() const { return name_; }
    bool nullable() const { return nullable_; }
    bool primaryKey() const { return primaryKey_; }
    bool unique() const { return unique_; }

protected:
    std::string name_;
    bool nullable_;
    bool primaryKey_;
    bool unique_;
    std::unique_ptr<DataType> defaultVal_;
};

// ---------------- BigIntColumn ----------------
class BigIntColumn : public Column {
public:
    BigIntColumn(std::string name,
                 bool nullable,
                 bool primaryKey,
                 bool unique,
                 int64_t defaultVal);

    void to_bits(BitBuffer &buf) const override;
    std::unique_ptr<DataType> parse(const std::string &raw) const override;
    std::unique_ptr<DataType> from_bits(const std::vector<uint8_t> &payload, size_t &ref) const override;

    ColumnType type() const override { return ColumnType::BIGINT; }
};

// ---------------- CharColumn ----------------
class CharColumn : public Column {
public:
    CharColumn(std::string name,
               uint32_t length,
               bool nullable,
               bool primaryKey,
               bool unique,
               std::string defaultVal);

    void to_bits(BitBuffer &buf) const override;
    std::unique_ptr<DataType> parse(const std::string &raw) const override;
    std::unique_ptr<DataType> from_bits(const std::vector<uint8_t> &payload, size_t &ref) const override;

    ColumnType type() const override { return ColumnType::CHAR; }

private:
    uint32_t length_;
};
