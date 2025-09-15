#pragma once
#include <unordered_map>
#include <memory>
#include <stdexcept>
#include <optional>
#include "dbone/columns/dataTypes.hpp"
#include "dbone/bitbuffer.hpp"
#include "dbone/schema.hpp"
#include "dbone/insert.hpp"
#include "dbone/columns/column.hpp"

class DataRow {
public:
    DataRow() = default;

    // Disable copying (unique_ptr cannot be copied)
    DataRow(const DataRow&) = delete;
    DataRow& operator=(const DataRow&) = delete;

    // Enable moving
    DataRow(DataRow&&) noexcept = default;
    DataRow& operator=(DataRow&&) noexcept = default;

    // API
    void set(size_t col, std::unique_ptr<DataType> val);
    const DataType& get(size_t col) const;

    void to_bits(BitBuffer &buf) const;
    size_t size() const { return values_.size(); }

    std::optional<size_t> primaryKeyIndex() const { return primaryKeyIndex_; }
    
    void print() const;



    // Conversion from Row + Schema
    static DataRow fromRow(const dbone::insert::Row &row, const TableSchema &schema);
    static DataRow bits_to_row(const std::vector<uint8_t>& payload, size_t& ref, const TableSchema& schema);


private:
    std::unordered_map<uint16_t, std::unique_ptr<DataType>> values_;
    std::optional<uint16_t> primaryKeyIndex_;
};

