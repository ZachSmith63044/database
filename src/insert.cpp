#include "dbone/insert.hpp"
#include "dbone/schema.hpp"
#include "dbone/storage.hpp"
#include "dbone/serialize.hpp"
#include "dbone/clustered_index_node.hpp"
#include <sstream>
#include <iostream>
#include <fstream>
#include <vector>
#include <iomanip>

struct InsertIntoResult {
    DataRow insertionRow;
    uint32_t page1;
    uint32_t page2;
};

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

    bool insertInto(const std::string &db_path, uint32_t page_num, const Row &row, uint32_t page_size, const TableSchema &schema)
    {
        // --- Read clustered index page ---
        std::ifstream in(db_path, std::ios::binary);
        if (!in)
        {
            return false;
        }

        uint64_t offset = static_cast<uint64_t>(page_num) * page_size;
        in.seekg(offset, std::ios::beg);

        if (!in)
        {
            return false;
        }

        // --- read root page ---
        std::vector<uint8_t> root(page_size);
        in.read(reinterpret_cast<char *>(root.data()), root.size());
        if (!in)
        {
            return false;
        }

        size_t off = 0;
        uint32_t page_count = readU32(root, off);

        // --- read page list ---
        std::vector<uint32_t> page_list;
        for (uint32_t i = 0; i < page_count; ++i)
        {
            uint32_t pg = readU32(root, off);
            page_list.push_back(pg);
        }

        
        // --- assemble full payload ---
        // header size = 4 + (page_count-1)*4
        size_t header_size = 4u + 4u * (page_count > 0 ? (page_count - 1) : 0);
        if (header_size > page_size)
        {
            return false;
        }

        std::vector<uint8_t> full_payload;

        // copy remainder of root page (after header)
        if (header_size < root.size())
        {
            full_payload.insert(full_payload.end(),
                                root.begin() + header_size, root.end());
        }

        // load and append each page in the list
        for (uint32_t pg : page_list)
        {
            uint64_t pg_off = static_cast<uint64_t>(pg) * page_size;
            std::vector<uint8_t> buf(page_size);

            in.seekg(pg_off, std::ios::beg);
            if (!in)
            {
                return false;
            }

            in.read(reinterpret_cast<char *>(buf.data()), buf.size());
            if (!in)
            {
                return false;
            }

            full_payload.insert(full_payload.end(), buf.begin(), buf.end());
        }

        // --- dump as hex ---

        // for (size_t i = 0; i < full_payload.size(); i += 16)
        // {
        //     std::cout << std::hex << std::setw(6) << std::setfill('0') << i << " : ";
        //     for (size_t j = 0; j < 16 && i + j < full_payload.size(); j++)
        //     {
        //         std::cout << std::setw(2) << std::setfill('0')
        //                   << static_cast<int>(full_payload[i + j]) << " ";
        //     }
        //     std::cout << "\n";
        // }
        // std::cout << std::dec;

        size_t ref = 4 * page_count;
        uint16_t nRows = readU32(full_payload, ref);


        ClusteredIndexNode clusteredIndexNode;
        clusteredIndexNode.set_available_pages(page_list);
        clusteredIndexNode.set_original_page(*schema.clustered_page_ref);
        clusteredIndexNode.add_pointer(readU32(full_payload, ref));

        for (size_t i = 0; i < nRows; i++)
        {
            DataRow row = DataRow::bits_to_row(full_payload, ref, schema);
            // row.print();
            clusteredIndexNode.add_row(std::move(row));
            clusteredIndexNode.add_pointer(readU32(full_payload, ref));
        }

        DataRow dataRow = DataRow::fromRow(row, schema);

        if (nRows == 0)
        {
            clusteredIndexNode.add_row(std::move(dataRow));
            clusteredIndexNode.add_pointer(static_cast<uint32_t>(0));
        }
        else
        {
            std::vector<DataRow> &rows = clusteredIndexNode.get_items();
            bool added = false;
            for (size_t i = 0; i < nRows; i++)
            {
                // dataRow.get(*dataRow.primaryKeyIndex());
                if (rows[i].get(*rows[i].primaryKeyIndex()) > dataRow.get(*dataRow.primaryKeyIndex()))
                {
                    clusteredIndexNode.add_row_at(std::move(dataRow), i);
                    clusteredIndexNode.add_pointer_at(static_cast<uint32_t>(0), i);
                    added = true;
                    break;
                }
                else if (rows[i].get(*rows[i].primaryKeyIndex()) == dataRow.get(*dataRow.primaryKeyIndex()))
                {
                    throw std::runtime_error(
                        "Insert failed: primary key already exists (value = " +
                        rows[i].get(*rows[i].primaryKeyIndex()).default_value_str() + ")");
                }
            }
            if (!added)
            {
                clusteredIndexNode.add_row(std::move(dataRow));
                clusteredIndexNode.add_pointer(static_cast<uint32_t>(0));
            }
        }
        // clusteredIndexNode.print();
        // clusteredIndexNode.print();
        // clusteredIndexNode.to_bits().printHex();
        clusteredIndexNode.save(db_path, schema, page_size);

        return true;
    }

    ValidationResult insert(const std::string &db_path, const Row &row, uint32_t page_size)
    {
        std::string err;
        TableSchema schema(read_schema(db_path, page_size));
        if (!err.empty())
        {
            return {false, "Failed to load schema: " + err};
        }

        ValidationResult validationResult = validate_row(schema, row);

        insertInto(db_path, *schema.clustered_page_ref, row, page_size, schema);

        return validationResult;
    }

} // namespace dbone::insert
