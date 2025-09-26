#pragma once
#include <string>
#include <memory>
#include <vector>
#include <optional>
#include "dbone/columns/column.hpp"
#include <unordered_map>

// Only declare create_table here
struct TableSchema
{
    TableSchema() = default;

    // delete copy constructor and copy assignment
    TableSchema(const TableSchema &) = delete;
    TableSchema &operator=(const TableSchema &) = delete;

    // allow move constructor and move assignment
    TableSchema(TableSchema &&) noexcept = default;
    TableSchema &operator=(TableSchema &&) noexcept = default;

    std::string table_name;
    std::vector<std::unique_ptr<Column>> columns;
    uint32_t min_length{};
    std::optional<uint32_t> clustered_page_ref;
    std::optional<uint32_t> available_pages_ref;
    std::unordered_map<size_t, uint32_t> index_page_refs{};
};

// pretty-print
inline std::ostream &operator<<(std::ostream &os, const TableSchema &schema)
{
    os << "TableSchema(" << schema.table_name << ")\n";
    os << "  min_length: " << schema.min_length << "\n";

    os << "  clustered_page_ref: ";
    if (schema.clustered_page_ref)
        os << *schema.clustered_page_ref;
    else
        os << "nullopt";
    os << "\n";

    os << "  available_pages_ref: ";
    if (schema.available_pages_ref)
        os << *schema.available_pages_ref;
    else
        os << "nullopt";
    os << "\n";

    os << "  index_page_refs:\n";
    for (auto &[k, v] : schema.index_page_refs)
    {
        os << "    key=" << k << " -> " << v << "\n";
    }

    os << "  columns:\n";
    for (size_t i = 0; i < schema.columns.size(); i++)
    {
        if (schema.columns[i])
        {
            os << "    [" << i << "] "
               << schema.columns[i]->name();

            const auto &def = schema.columns[i]->default_val();
            if (def)
            {
                os << " : default=" << def->type_name()
                   << " (" << def->default_value_str() << ")";
            }
            else
            {
                os << " : default=<none>";
            }

            os << "\n";
        }
        else
        {
            os << "    [" << i << "] <null column>\n";
        }
    }

    return os;
};

bool create_table(const TableSchema &s,
                  const std::string &path,
                  std::string *err,
                  std::uint32_t page_size);

TableSchema read_schema(const std::string &path, uint32_t page_size);
