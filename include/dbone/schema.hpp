#pragma once
#include <string>
#include <memory>
#include <vector>
#include <optional>
#include "dbone/columns/column.hpp"

// Only declare create_table here
struct TableSchema {
    TableSchema() = default;

    // delete copy constructor and copy assignment
    TableSchema(const TableSchema&) = delete;
    TableSchema& operator=(const TableSchema&) = delete;

    // allow move constructor and move assignment
    TableSchema(TableSchema&&) noexcept = default;
    TableSchema& operator=(TableSchema&&) noexcept = default;

    std::string table_name;
    std::vector<std::unique_ptr<Column>> columns;
    uint32_t min_length{};
    std::optional<uint32_t> clustered_page_ref;
};


bool create_table(const TableSchema &s,
                  const std::string &path,
                  std::string *err,
                  std::uint32_t page_size);

TableSchema read_schema(const std::string &path, uint32_t page_size);
