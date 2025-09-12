#pragma once
#include <string>
#include <memory>
#include "dbone/bitbuffer.hpp"
#include "dbone/columns/dataTypes.hpp"

class Column {
public:
    Column(std::string name, bool nullable, bool primaryKey, bool unique,
           std::unique_ptr<DataType> defaultVal);

    virtual ~Column() = default;

    // serialize schema (name, flags, type, default value)
    virtual void to_bits(BitBuffer &buf) const = 0;

    virtual std::string default_value_str() const {
        return defaultVal_ ? defaultVal_->default_value_str() : "NULL";
    }

    // --- Add getters ---
    const std::string& name() const { return name_; }
    bool nullable() const { return nullable_; }
    bool primaryKey() const { return primaryKey_; }
    bool unique() const { return unique_; }

protected:
    std::string name_;
    bool nullable_;
    bool primaryKey_;
    bool unique_;
    std::unique_ptr<DataType> defaultVal_;  // << new: delegates to dataTypes
};

// ---------------- BigIntColumn ----------------
class BigIntColumn : public Column {
public:
    BigIntColumn(std::string name, bool nullable, bool primaryKey, bool unique, int64_t defaultVal);

    void to_bits(BitBuffer &buf) const override;
};

// ---------------- CharColumn ----------------
class CharColumn : public Column {
public:
    CharColumn(std::string name, uint32_t length,
               bool nullable, bool primaryKey, bool unique, std::string defaultVal);

    void to_bits(BitBuffer &buf) const override;

private:
    uint32_t length_;
};
