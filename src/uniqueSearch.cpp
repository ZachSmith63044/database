#include "dbone/uniqueSearch.hpp"
#include <dbone/row.hpp>
#include <dbone/secondary_index_node.hpp>

bool searchIndexedAcc(const std::string &db_path, const TableSchema &schema,
                      uint32_t currentPage, const DataType &searching,
                      const Column &indexed_col, const Column &pk_col,
                      uint32_t page_size)
{
    std::cout << "SEARCHING PAGE " << currentPage << std::endl;
    SecondaryIndexNode secondaryIndexNode = SecondaryIndexNode::load(db_path, currentPage, schema, indexed_col, pk_col, page_size);
    std::vector<IndexEntry> entries = secondaryIndexNode.entries();

    bool checkEnd = true;

    for (size_t i = 0; i < entries.size(); i++)
    {
        if (*entries[i].value > searching)
        {
            checkEnd = false;
            if (secondaryIndexNode.page_pointers()[i] != 0)
            {
                // ✅ propagate result
                return searchIndexedAcc(db_path, schema,
                                        secondaryIndexNode.page_pointers()[i],
                                        searching, indexed_col, pk_col, page_size);
            }
            else
            {
                return false;
            }
        }
        else if (*entries[i].value == searching)
        {
            std::cout << "EQUAL" << std::endl;
            return true;
        }
    }

    if (checkEnd)
    {
        if (secondaryIndexNode.page_pointers()[entries.size()] != 0)
        {
            // ✅ propagate result
            return searchIndexedAcc(db_path, schema,
                                    secondaryIndexNode.page_pointers()[entries.size()],
                                    searching, indexed_col, pk_col, page_size);
        }
    }

    return false;
}


bool searchUnique(const std::string &db_path, const TableSchema &schema,
                  const std::unordered_map<std::string, std::string> &row,
                  uint32_t page_size)
{
    std::unordered_map<uint16_t, std::unique_ptr<Column>> primaryColumns;

    for (uint16_t i = 0; i < schema.columns.size(); i++)
    {
        if (schema.columns[i]->primaryKey())
        {
            primaryColumns[i] = schema.columns[i]->clone(); // deep copy
        }
    }

    Column* pk_column = nullptr;
    if (!primaryColumns.empty())
    {
        pk_column = primaryColumns.begin()->second.get();
    }

    DataRow dataRow = DataRow::fromRow(row, schema);

    for (const auto &[key, value] : schema.index_page_refs)
    {
        if (schema.columns[key]->indexed() && pk_column != nullptr)
        {
            std::cout << "SEARCHING KEY: " << key
                      << " for value: " << dataRow.get(key).default_value_str()
                      << " at page " << value << std::endl;

            bool found = searchIndexedAcc(db_path, schema, value,
                                          dataRow.get(key),
                                          *schema.columns[key],
                                          *pk_column,
                                          page_size);
            std::cout << found << std::endl;

            if (found == true)
            {
                std::cout << "TRUE>" << std::endl;
            }

            if (found)  // if a duplicate was found
                return true;
        }
    }

    return false;
}
