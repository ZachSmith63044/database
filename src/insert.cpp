#include "dbone/insert.hpp"
#include "dbone/schema.hpp"
// #include "dbone/columns/column.hpp"
#include <sstream>
#include <iostream>

namespace dbone::insert
{

    ValidationResult validate_row(const TableSchema &schema, const Row &row)
    {
        // First, check required + declared columns
        for (const auto &colPtr : schema.columns)
        {
            const Column &col = *colPtr;

            auto it = row.find(col.name());
            bool provided = (it != row.end() && !it->second.empty());

            if (!col.nullable() && !provided)
            {
                std::ostringstream oss;
                oss << "Column '" << col.name() << "' is NOT NULL but missing in insert";
                return {false, oss.str()};
            }
        }

        // Second, check for extra fields not in schema
        for (const auto &kv : row)
        {
            bool found = false;
            for (const auto &colPtr : schema.columns)
            {
                if (kv.first == colPtr->name())
                {
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                std::ostringstream oss;
                oss << "Column '" << kv.first << "' does not exist in table schema";
                return {false, oss.str()};
            }
        }

        return {true, ""};
    }

    ValidationResult insert(const std::string &db_path, const Row &row, uint32_t page_size)
    {
        std::string err;
        TableSchema schema = read_schema(db_path, page_size);
        if (!err.empty())
        {
            return {false, "Failed to load schema: " + err};
        }

        return validate_row(schema, row);
    }

} // namespace dbone::insert
