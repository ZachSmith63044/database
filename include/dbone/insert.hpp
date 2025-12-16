#pragma once
#include "dbone/schema.hpp"
#include "row_data.hpp"
#include <string>
#include <unordered_map>

namespace dbone::insert
{

    struct ValidationResult
    {
        bool ok;
        std::string error;
    };

    /// Validate row against schema rules
    ValidationResult validate_row(const TableSchema &schema, const Row &row);

    /// Insert wrapper:
    /// - Loads schema from path
    /// - Calls validate_row
    ValidationResult insert(const std::string &db_path, const Row &row, uint32_t page_size);

} // namespace dbone::insert
