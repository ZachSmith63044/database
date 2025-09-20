#pragma once
#include <string>
#include <memory>
#include <vector>
#include <optional>
#include "dbone/columns/column.hpp"
#include <unordered_map>

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
    std::optional<uint32_t> available_pages_ref;
    std::unordered_map<size_t, uint32_t> index_page_refs{};
};


bool create_table(const TableSchema &s,
                  const std::string &path,
                  std::string *err,
                  std::uint32_t page_size);

TableSchema read_schema(const std::string &path, uint32_t page_size);
