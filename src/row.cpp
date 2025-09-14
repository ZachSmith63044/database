#include "dbone/row.hpp"
#include "dbone/serialize.hpp"
#include <iostream>

void DataRow::set(size_t col, std::unique_ptr<DataType> val)
{
    values_[col] = std::move(val);
}

const DataType &DataRow::get(size_t col) const
{
    auto it = values_.find(col);
    if (it == values_.end() || !it->second)
    {
        throw std::out_of_range("Row::get - column not found or null");
    }
    return *it->second;
}

void DataRow::to_bits(BitBuffer &buf) const
{
    buf.putU16(static_cast<uint16_t>(values_.size()));

    for (const auto &pair : values_)
    {
        size_t col = pair.first;
        const auto &val = pair.second;

        if (!val)
        {
            throw std::runtime_error("Row::to_bits - null value in row");
        }

        buf.putU16(static_cast<uint16_t>(col));
        val->to_bits(buf);
    }
}

// Convert Row (string->string) into DataRow using TableSchema
DataRow DataRow::fromRow(const dbone::insert::Row &row, const TableSchema &schema)
{
    DataRow dr;

    for (size_t i = 0; i < schema.columns.size(); i++)
    {
        const auto &col = schema.columns[i];

        // Find the value in Row
        auto it = row.find(col->name());
        if (it == row.end())
        {
            throw std::runtime_error("Missing value for column: " + col->name());
        }

        // Convert string -> DataType
        std::unique_ptr<DataType> dt = col->parse(it->second);
        dr.set(i, std::move(dt));

        // Track primary key index
        if (col->primaryKey())
        {
            dr.primaryKeyIndex_ = i;
        }
    }

    return std::move(dr); // ✅ force move, avoids deleted copy error
}

DataRow DataRow::bits_to_row(const std::vector<uint8_t> &payload,
                             size_t &ref,
                             const TableSchema &schema)
{
    DataRow dr;

    uint16_t rowLength = readU16(payload, ref);
    std::cout << rowLength << std::endl;
    for (size_t i = 0; i < rowLength; i++)
    {
        uint16_t columnNum = readU16(payload, ref);
        std::cout << columnNum << std::endl;
        dr.set(columnNum, schema.columns[columnNum].get()->from_bits(payload, ref));
        // schema.columns[columnNum].get().;
        if (schema.columns[columnNum].get()->primaryKey())
        {
            dr.primaryKeyIndex_ = columnNum;
        }
    }

    return dr;
}

void DataRow::print() const
{
    std::cout << "{ ";
    for (auto it = values_.begin(); it != values_.end(); ++it)
    {
        std::cout << "col" << it->first << "=" << it->second->default_value_str();
        if (std::next(it) != values_.end())
        {
            std::cout << ", ";
        }
    }
    std::cout << " }";
    if (primaryKeyIndex_)
    {
        std::cout << " (pk=" << *primaryKeyIndex_ << ")";
    }
    std::cout << std::endl;
}