#pragma once
#include "dbone/schema.hpp"
#include <string>
#include <unordered_map>
#include "dbone/insert.hpp"
#include "dbone/row.hpp"
#include <vector>

struct SearchResult
{
    std::vector<dbone::insert::Row> rows;
    long long timeTaken;
};

enum class Comparator {
    Less,        // <
    LessEqual,   // <=
    Equal,       // ==
    GreaterEqual,// >=
    Greater      // >
};

inline std::ostream& operator<<(std::ostream& os, Comparator c) {
    switch (c) {
        case Comparator::Equal:        return os << "=";
        case Comparator::Less:         return os << "<";
        case Comparator::Greater:      return os << ">";
        case Comparator::LessEqual:    return os << "<=";
        case Comparator::GreaterEqual: return os << ">=";
    }
    return os << "unknown";
}

struct SearchParam
{
    std::string columnName;
    std::optional<uint16_t> columnIndex;
    Comparator comparator;
    std::unique_ptr<DataType> compareTo;
};

namespace dbone::search
{

    /// Insert wrapper:
    /// - Loads schema from path
    /// - Calls validate_row
    SearchResult searchItem(const std::string &db_path, const std::vector<SearchParam>& queries, uint32_t page_size);
    
    SearchResult searchPrimaryKeys(const std::string &db_path, std::vector<std::unique_ptr<DataType>> &primaryKeys, uint32_t page_size);

} // namespace dbone::insert
