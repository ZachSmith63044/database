#include "dbone/columns/column.hpp"

// ---------- Column ----------
Column::Column(std::string name, bool nullable, bool primaryKey, bool unique,
               std::unique_ptr<DataType> defaultVal)
    : name_(std::move(name)), nullable_(nullable), primaryKey_(primaryKey),
      unique_(unique), defaultVal_(std::move(defaultVal)) {}

// ---------- BigIntColumn ----------
BigIntColumn::BigIntColumn(std::string name, bool nullable, bool primaryKey, bool unique, int64_t defaultVal)
    : Column(std::move(name), nullable, primaryKey, unique,
             std::make_unique<BigIntType>(defaultVal)) {}

void BigIntColumn::to_bits(BitBuffer &buf) const {
    // name
    buf.putString(name_);

    // flags + type
    uint8_t packed = 0;
    if (nullable_)   packed |= (1u << 7);
    if (primaryKey_) packed |= (1u << 6);
    if (unique_)     packed |= (1u << 5);
    packed |= 1; // type code for BIGINT
    buf.putU8(packed);

    // default (via DataType)
    defaultVal_->to_bits(buf);
}

// ---------- CharColumn ----------
CharColumn::CharColumn(std::string name, uint32_t length,
                       bool nullable, bool primaryKey, bool unique, std::string defaultVal)
    : Column(std::move(name), nullable, primaryKey, unique,
             std::make_unique<CharType>(defaultVal, length)),
      length_(length) {}

void CharColumn::to_bits(BitBuffer &buf) const {
    buf.putString(name_);

    uint8_t packed = 0;
    if (nullable_)   packed |= (1u << 7);
    if (primaryKey_) packed |= (1u << 6);
    if (unique_)     packed |= (1u << 5);
    packed |= 2; // type code for CHAR(N)
    buf.putU8(packed);

    // length
    buf.putU32(length_);

    // default (via DataType)
    defaultVal_->to_bits(buf);
}
