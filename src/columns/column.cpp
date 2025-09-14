#include "dbone/columns/column.hpp"

// ---------- Column ----------
Column::Column(std::string name,
               bool nullable,
               bool primaryKey,
               bool unique,
               std::unique_ptr<DataType> defaultVal)
    : name_(std::move(name)),
      nullable_(nullable),
      primaryKey_(primaryKey),
      unique_(unique),
      defaultVal_(std::move(defaultVal)) {}

// ---------- BigIntColumn ----------
BigIntColumn::BigIntColumn(std::string name,
                           bool nullable,
                           bool primaryKey,
                           bool unique,
                           int64_t defaultVal)
    : Column(std::move(name),
             nullable,
             primaryKey,
             unique,
             std::make_unique<BigIntType>(defaultVal)) {}

void BigIntColumn::to_bits(BitBuffer &buf) const {
    buf.putString(name_);

    uint8_t packed = 0;
    if (nullable_)   packed |= (1u << 7);
    if (primaryKey_) packed |= (1u << 6);
    if (unique_)     packed |= (1u << 5);
    packed |= static_cast<uint8_t>(ColumnType::BIGINT);
    buf.putU8(packed);

    defaultVal_->to_bits(buf);
}

std::unique_ptr<DataType> BigIntColumn::parse(const std::string &raw) const {
    return BigIntType::parse(raw);
}

std::unique_ptr<DataType> BigIntColumn::from_bits(const std::vector<uint8_t> &payload, size_t &ref) const {
    return std::make_unique<BigIntType>(BigIntType::from_bits(payload, ref));
}

// ---------- CharColumn ----------
CharColumn::CharColumn(std::string name,
                       uint32_t length,
                       bool nullable,
                       bool primaryKey,
                       bool unique,
                       std::string defaultVal)
    : Column(std::move(name),
             nullable,
             primaryKey,
             unique,
             std::make_unique<CharType>(defaultVal, length)),
      length_(length) {}

void CharColumn::to_bits(BitBuffer &buf) const {
    buf.putString(name_);

    uint8_t packed = 0;
    if (nullable_)   packed |= (1u << 7);
    if (primaryKey_) packed |= (1u << 6);
    if (unique_)     packed |= (1u << 5);
    packed |= static_cast<uint8_t>(ColumnType::CHAR);
    buf.putU8(packed);

    buf.putU32(length_);

    defaultVal_->to_bits(buf);
}

std::unique_ptr<DataType> CharColumn::parse(const std::string &raw) const {
    return CharType::parse(raw, length_);
}

std::unique_ptr<DataType> CharColumn::from_bits(const std::vector<uint8_t> &payload, size_t &ref) const {
    return std::make_unique<CharType>(CharType::from_bits(payload, ref, length_));
}
