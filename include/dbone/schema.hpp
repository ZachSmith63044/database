#pragma once
#include <string>
#include <memory>
#include <vector>
#include "dbone/columns/column.hpp"

// Only declare create_table here
struct TableSchema {
    std::string table_name;
    std::vector<std::unique_ptr<Column>> columns;
};

bool create_table(const TableSchema &s,
                  const std::string &path,
                  std::string *err,
                  std::uint32_t page_size);

TableSchema read_schema(const std::string &path, uint32_t page_size);
