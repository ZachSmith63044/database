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

struct InsertIntoResult
{
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

    uint32_t split_root(uint32_t current_page_ref, ClusteredIndexNode &originalNode, const TableSchema &schema, const std::string &db_path, uint32_t page_size)
    {
        std::cout << "1" << std::endl;
        std::vector<uint32_t> otherAvailablePages = originalNode.get_available_pages_index();

        ClusteredIndexNode newRoot;
        newRoot.add_row(std::move(originalNode.get_items()[schema.min_length]));
        newRoot.add_pointer(0);
        newRoot.add_pointer(0);
        newRoot.set_original_page(*originalNode.get_original_page());
        newRoot.set_available_pages(otherAvailablePages);

        std::cout << "2" << std::endl;
        std::vector<uint32_t> pagesUsed = newRoot.save(db_path, schema, page_size, false);
        std::cout << "2.5" << std::endl;
        for (uint32_t page : pagesUsed)
        {
            otherAvailablePages.erase(
                std::remove(otherAvailablePages.begin(), otherAvailablePages.end(), page),
                otherAvailablePages.end());
        }

        // newRoot.set_original_page(originalNode.get_original_page());

        std::cout << "3" << std::endl;
        ClusteredIndexNode page1;
        ClusteredIndexNode page2;
        for (int i = 0; i < schema.min_length; i++)
        {
            page1.add_row(std::move(originalNode.get_items()[i]));
            page1.add_pointer(originalNode.get_page_pointers()[i]);
            page2.add_row(std::move(originalNode.get_items()[i + 1 + schema.min_length]));
            page2.add_pointer(originalNode.get_page_pointers()[i + schema.min_length + 1]);
        }
        page1.add_pointer(originalNode.get_page_pointers()[schema.min_length]);
        page2.add_pointer(originalNode.get_page_pointers()[schema.min_length * 2]);

        std::cout << "4" << std::endl;

        uint32_t page1Ptr;
        if (otherAvailablePages.size() > 0)
        {
            page1Ptr = otherAvailablePages[0];
            otherAvailablePages.erase(otherAvailablePages.begin());
        }
        else
        {
            page1Ptr = num_pages_in_file(db_path, page_size);
        }
        page1.set_original_page(page1Ptr);
        page1.set_available_pages(otherAvailablePages);
        std::vector<uint32_t> pagesUsed1 = page1.save(db_path, schema, page_size);
        for (uint32_t page : pagesUsed1)
        {
            otherAvailablePages.erase(
                std::remove(otherAvailablePages.begin(), otherAvailablePages.end(), page),
                otherAvailablePages.end());
        }

        std::cout << "5" << std::endl;
        uint32_t page2Ptr;
        if (otherAvailablePages.size() > 0)
        {
            page2Ptr = otherAvailablePages[0];
            otherAvailablePages.erase(otherAvailablePages.begin());
        }
        else
        {
            page2Ptr = num_pages_in_file(db_path, page_size);
        }
        page2.set_original_page(page2Ptr);
        page2.set_available_pages(otherAvailablePages);
        std::vector<uint32_t> pagesUsed2 = page2.save(db_path, schema, page_size);

        std::cout << "6" << std::endl;
        newRoot.clear_pointers();
        newRoot.add_pointer(page1Ptr);
        newRoot.add_pointer(page2Ptr);
        newRoot.save(db_path, schema, page_size);

        std::cout << page1.get_items().size() << std::endl;
        std::cout << page2.get_items().size() << std::endl;

        return current_page_ref;
    }

    bool insert_data_row(const std::string &db_path, uint32_t page_num, DataRow &&row, uint32_t page1, uint32_t page2, uint32_t page_size, const TableSchema &schema)
    {
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

        std::vector<uint8_t> root(page_size);
        in.read(reinterpret_cast<char *>(root.data()), root.size());
        if (!in)
        {
            return false;
        }

        size_t off = 0;
        uint32_t page_count = readU32(root, off);

        std::vector<uint32_t> page_list;
        for (uint32_t i = 0; i < page_count; ++i)
        {
            uint32_t pg = readU32(root, off);
            page_list.push_back(pg);
        }

        size_t header_size = 4u + 4u * (page_count > 0 ? (page_count) : 0);
        if (header_size > page_size)
        {
            return false;
        }

        std::vector<uint8_t> full_payload;

        if (header_size < root.size())
        {
            full_payload.insert(full_payload.end(),
                                root.begin() + header_size, root.end());
        }

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
        size_t ref = 0;
        uint16_t nRows = readU32(full_payload, ref);

        ClusteredIndexNode clusteredIndexNode;
        clusteredIndexNode.set_available_pages(page_list);
        clusteredIndexNode.set_original_page(page_num);
        clusteredIndexNode.add_pointer(readU32(full_payload, ref));

        for (size_t i = 0; i < nRows; i++)
        {
            DataRow row = DataRow::bits_to_row(full_payload, ref, schema);
            clusteredIndexNode.add_row(std::move(row));
            clusteredIndexNode.add_pointer(readU32(full_payload, ref));
        }

        if (clusteredIndexNode.get_items().size() >= schema.min_length * 2 + 1)
        {
            throw std::runtime_error("INSERT FAILED - FORCE INSERT BREAKS BOUNDS");
        }
        DataRow &&dataRow = std::move(row);
        std::cout << "WE GET HERE?" << std::endl;
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
                    clusteredIndexNode.add_pointer_at(static_cast<uint32_t>(page1), i);
                    clusteredIndexNode.set_pointer_at(static_cast<uint32_t>(page2), i + 1);
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
                clusteredIndexNode.set_pointer_at(page1, clusteredIndexNode.get_items().size() - 1);
                clusteredIndexNode.add_pointer(page2);
            }
        }
        // clusteredIndexNode.print();
        // clusteredIndexNode.print();
        // clusteredIndexNode.to_bits().printHex();
        std::cout << "CLUSTER SAVE: " << page_num << std::endl;
        std::cout << *clusteredIndexNode.get_original_page() << std::endl;
        clusteredIndexNode.save(db_path, schema, page_size);

        return true;
    }

    bool split_node(uint32_t above_page_ref, ClusteredIndexNode &originalNode, const TableSchema &schema, const std::string &db_path, uint32_t page_size)
    {
        std::cout << "1" << std::endl;

        std::vector<uint32_t> otherAvailablePages = originalNode.get_available_pages_index();
        otherAvailablePages.insert(otherAvailablePages.begin(), *originalNode.get_original_page());

        DataRow &&rowPush = std::move(originalNode.get_items()[schema.min_length]);

        // newRoot.set_original_page(originalNode.get_original_page());

        std::cout << "3" << std::endl;
        ClusteredIndexNode page1;
        ClusteredIndexNode page2;
        for (int i = 0; i < schema.min_length; i++)
        {
            page1.add_row(std::move(originalNode.get_items()[i]));
            page1.add_pointer(originalNode.get_page_pointers()[i]);
            page2.add_row(std::move(originalNode.get_items()[i + 1 + schema.min_length]));
            page2.add_pointer(originalNode.get_page_pointers()[i + schema.min_length + 1]);
        }
        page1.add_pointer(originalNode.get_page_pointers()[schema.min_length]);
        page2.add_pointer(originalNode.get_page_pointers()[schema.min_length * 2]);

        std::cout << "4" << std::endl;

        uint32_t page1Ptr;
        if (otherAvailablePages.size() > 0)
        {
            page1Ptr = otherAvailablePages[0];
            otherAvailablePages.erase(otherAvailablePages.begin());
        }
        else
        {
            page1Ptr = num_pages_in_file(db_path, page_size);
        }
        page1.set_original_page(page1Ptr);
        page1.set_available_pages(otherAvailablePages);
        std::vector<uint32_t> pagesUsed1 = page1.save(db_path, schema, page_size);
        for (uint32_t page : pagesUsed1)
        {
            otherAvailablePages.erase(
                std::remove(otherAvailablePages.begin(), otherAvailablePages.end(), page),
                otherAvailablePages.end());
        }

        std::cout << "5" << std::endl;
        uint32_t page2Ptr;
        if (otherAvailablePages.size() > 0)
        {
            page2Ptr = otherAvailablePages[0];
            otherAvailablePages.erase(otherAvailablePages.begin());
        }
        else
        {
            page2Ptr = num_pages_in_file(db_path, page_size);
        }
        page2.set_original_page(page2Ptr);
        page2.set_available_pages(otherAvailablePages);
        std::vector<uint32_t> pagesUsed2 = page2.save(db_path, schema, page_size);

        std::cout << "6" << std::endl;
        // newRoot.clear_pointers();
        // newRoot.add_pointer(page1Ptr);
        // newRoot.add_pointer(page2Ptr);
        // newRoot.save(db_path, schema, page_size);

        insert_data_row(db_path, above_page_ref, std::move(rowPush), page1Ptr, page2Ptr, page_size, schema);

        std::cout << page1.get_items().size() << std::endl;
        std::cout << page2.get_items().size() << std::endl;

        return true;

        // return current_page_ref;
    }

    bool insertInto(const std::string &db_path, uint32_t page_num, const Row &row, uint32_t page_size, const TableSchema &schema, uint32_t previous_page_ref = 0)
    {
        // InsertIntoResult insertionResult;
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
        size_t header_size = 4u + 4u * (page_count > 0 ? (page_count) : 0);
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

        // off = 0;

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

        size_t ref = 0;
        uint16_t nRows = readU32(full_payload, ref);

        ClusteredIndexNode clusteredIndexNode;
        clusteredIndexNode.set_available_pages(page_list);
        clusteredIndexNode.set_original_page(page_num);
        clusteredIndexNode.add_pointer(readU32(full_payload, ref));

        for (size_t i = 0; i < nRows; i++)
        {
            DataRow row = DataRow::bits_to_row(full_payload, ref, schema);
            // row.print();
            clusteredIndexNode.add_row(std::move(row));
            clusteredIndexNode.add_pointer(readU32(full_payload, ref));
        }

        if (clusteredIndexNode.get_items().size() >= schema.min_length * 2 + 1)
        {
            std::cout << "SPLIT NODE" << std::endl;
            if (previous_page_ref == 0)
            {
                uint32_t pointer = split_root(*clusteredIndexNode.get_original_page(), clusteredIndexNode, schema, db_path, page_size);
            }
            else
            {
                bool pointer = split_node(previous_page_ref, clusteredIndexNode, schema, db_path, page_size);
            }
            std::cout << "SHOULD INSERT" << std::endl;
            return false;
            // insertInto(db_path, pointer, row, page_size, schema);
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
                    if (clusteredIndexNode.get_page_pointers()[0] == static_cast<uint32_t>(0))
                    {
                        clusteredIndexNode.add_row_at(std::move(dataRow), i);
                        clusteredIndexNode.add_pointer_at(static_cast<uint32_t>(0), i);
                    }
                    else
                    {
                        return insertInto(db_path, clusteredIndexNode.get_page_pointers()[i], row, page_size, schema, page_num);
                    }
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
                if (clusteredIndexNode.get_page_pointers()[0] == static_cast<uint32_t>(0))
                {
                    clusteredIndexNode.add_row(std::move(dataRow));
                    clusteredIndexNode.add_pointer(static_cast<uint32_t>(0));
                }
                else
                {
                    return insertInto(db_path, clusteredIndexNode.get_page_pointers()[clusteredIndexNode.get_page_pointers().size() - 1], row, page_size, schema, page_num);
                }
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
