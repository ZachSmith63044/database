#include "dbone/columns/column.hpp"
#include <optional>

// ---------- Column ----------
Column::Column(std::string name,
               bool nullable,
               bool primaryKey,
               bool unique,
               bool indexed,
               std::unique_ptr<DataType> defaultVal)
    : name_(std::move(name)),
      nullable_(nullable),
      primaryKey_(primaryKey),
      unique_(unique),
      indexed_(indexed),
      defaultVal_(std::move(defaultVal)) {}

// ---------- BigIntColumn ----------
BigIntColumn::BigIntColumn(std::string name,
                           bool nullable,
                           bool primaryKey,
                           bool unique,
                           bool indexed,
                           int64_t defaultVal)
    : Column(std::move(name),
             nullable,
             primaryKey,
             unique,
             indexed,
             std::make_unique<BigIntType>(defaultVal)) {}

void BigIntColumn::to_bits(BitBuffer &buf) const
{
    buf.putString(name_);

    uint8_t packed = 0;
    if (nullable_)
        packed |= (1u << 7);
    if (primaryKey_)
        packed |= (1u << 6);
    if (unique_)
        packed |= (1u << 5);
    if (indexed_)
        packed |= (1u << 4);
    packed |= static_cast<uint8_t>(ColumnType::BIGINT);
    buf.putU8(packed);

    defaultVal_->to_bits(buf);
}

std::unique_ptr<DataType> BigIntColumn::parse(const std::string &raw) const
{
    return BigIntType::parse(raw);
}

std::unique_ptr<DataType> BigIntColumn::from_bits(const std::vector<uint8_t> &payload, size_t &ref) const
{
    return std::make_unique<BigIntType>(BigIntType::from_bits(payload, ref));
}

// ---------- CharColumn ----------
CharColumn::CharColumn(std::string name,
                       uint32_t length,
                       bool nullable,
                       bool primaryKey,
                       bool unique,
                       bool indexed,
                       std::string defaultVal)
    : Column(std::move(name),
             nullable,
             primaryKey,
             unique,
             indexed,
             std::make_unique<CharType>(defaultVal, length)),
      length_(length) {}

void CharColumn::to_bits(BitBuffer &buf) const
{
    buf.putString(name_);

    uint8_t packed = 0;
    if (nullable_)
        packed |= (1u << 7);
    if (primaryKey_)
        packed |= (1u << 6);
    if (unique_)
        packed |= (1u << 5);
    if (indexed_)
        packed |= (1u << 4);
    packed |= static_cast<uint8_t>(ColumnType::CHAR);
    buf.putU8(packed);

    buf.putU32(length_);

    defaultVal_->to_bits(buf);
}

std::unique_ptr<DataType> CharColumn::parse(const std::string &raw) const
{
    return CharType::parse(raw, length_);
}

std::unique_ptr<DataType> CharColumn::from_bits(const std::vector<uint8_t> &payload, size_t &ref) const
{
    return std::make_unique<CharType>(CharType::from_bits(payload, ref, length_));
}

// ---------- VarCharColumn ----------
VarCharColumn::VarCharColumn(std::string name,
                       uint32_t max_length,
                       bool nullable,
                       bool primaryKey,
                       bool unique,
                       bool indexed,
                       std::string defaultVal)
    : Column(std::move(name),
             nullable,
             primaryKey,
             unique,
             indexed,
             std::make_unique<VarCharType>(defaultVal, max_length)),
      max_length_(max_length) {}

void VarCharColumn::to_bits(BitBuffer &buf) const
{
    buf.putString(name_);

    uint8_t packed = 0;
    if (nullable_)
        packed |= (1u << 7);
    if (primaryKey_)
        packed |= (1u << 6);
    if (unique_)
        packed |= (1u << 5);
    if (indexed_)
        packed |= (1u << 4);
    packed |= static_cast<uint8_t>(ColumnType::VARCHAR);
    buf.putU8(packed);

    buf.putU32(max_length_);

    defaultVal_->to_bits(buf);
}

std::unique_ptr<DataType> VarCharColumn::parse(const std::string &raw) const
{
    return VarCharType::parse(raw, max_length_);
}

std::unique_ptr<DataType> VarCharColumn::from_bits(const std::vector<uint8_t> &payload, size_t &ref) const
{
    return std::make_unique<VarCharType>(VarCharType::from_bits(payload, ref, max_length_));
}
