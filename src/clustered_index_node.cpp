#include "dbone/clustered_index_node.hpp"
#include <stdexcept>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <filesystem>
#include "dbone/availablePages.hpp"
#include "dbone/clustered_index_node.hpp"
// Add a row
void ClusteredIndexNode::add_row(DataRow &&row)
{
    items_.push_back(std::move(row));
}

void ClusteredIndexNode::add_row_at(DataRow &&row, size_t position)
{
    if (position > items_.size())
    {
        throw std::out_of_range("ClusteredIndexNode::add_row_at: position out of range");
    }

    items_.insert(items_.begin() + position, std::move(row));
}

// Add a page pointer
void ClusteredIndexNode::add_pointer(uint32_t page_pointer)
{
    page_pointers_.push_back(page_pointer);
}

// Add a page pointer
void ClusteredIndexNode::clear_pointers()
{
    std::vector<uint32_t> newPtrs;
    page_pointers_ = newPtrs;
}

void ClusteredIndexNode::add_pointer_at(uint32_t page_pointer, size_t position)
{
    if (position > page_pointers_.size())
    {
        throw std::out_of_range("ClusteredIndexNode::add_pointer_at: position out of range");
    }

    page_pointers_.insert(page_pointers_.begin() + position, page_pointer);
}

void ClusteredIndexNode::set_pointer_at(uint32_t page_pointer, size_t position)
{
    if (position > page_pointers_.size())
    {
        throw std::out_of_range("ClusteredIndexNode::add_pointer_at: position out of range");
    }

    page_pointers_[position] = page_pointer;
}

// Set original page
void ClusteredIndexNode::set_original_page(uint32_t page)
{
    original_page_ = page;
    if (page >= next_new_page_id_)
    {
        next_new_page_id_ = page + 1;
    }
}

// Set available pages
void ClusteredIndexNode::set_available_pages(const std::vector<uint32_t> &pages)
{
    available_pages_ = pages;
    for (uint32_t p : pages)
    {
        if (p >= next_new_page_id_)
        {
            next_new_page_id_ = p + 1;
        }
    }
}

// Set available pages
void ClusteredIndexNode::set_extra_available_pages(const std::vector<uint32_t> &pages)
{
    extra_available_pages_ = pages;
}

std::vector<DataRow> &ClusteredIndexNode::get_items()
{
    return items_;
}

std::vector<uint32_t> &ClusteredIndexNode::get_page_pointers()
{
    return page_pointers_;
}

// Build payload buffer
BitBuffer ClusteredIndexNode::to_bits() const
{
    BitBuffer buf;

    // number of rows
    buf.putU32(static_cast<uint32_t>(items_.size()));

    buf.putU32(page_pointers_[0]);

    // rows
    for (size_t i = 0; i < items_.size(); i++)
    {
        items_[i].to_bits(buf);
        buf.putU32(page_pointers_[i + 1]);
    }
    // for (const auto &row : items_)
    // {
    //     row.to_bits(buf);
    // }

    return buf;
}

size_t num_pages_in_file(const std::string &path, size_t page_size)
{
    namespace fs = std::filesystem;
    auto file_size = fs::file_size(path);
    return file_size / page_size;
}

std::vector<uint32_t> ClusteredIndexNode::get_available_pages_index()
{
    return available_pages_;
}

std::optional<uint32_t> ClusteredIndexNode::get_original_page()
{
    return original_page_;
}

std::vector<uint32_t> ClusteredIndexNode::save(const std::string &db_path, const TableSchema &schema, uint32_t page_size, bool save)
{
    if (!original_page_)
    {
        throw std::runtime_error("ClusteredIndexNode::save: original page not set");
    }

    // Build payload of all rows
    BitBuffer payload = to_bits();
    const auto &data = payload.bytes();

    // Start with a guess for number of pages
    size_t num_pages_needed = 1;
    while (true)
    {
        // Header = 4B count + 4B for each extra page
        size_t header_size = 4 + 4 * (num_pages_needed - 1);
        size_t total_size = header_size + data.size();

        size_t new_num_pages = (total_size + page_size - 1) / page_size;
        if (new_num_pages == num_pages_needed)
            break;
        num_pages_needed = new_num_pages;
    }

    // Build page allocation order
    std::vector<uint32_t> used_pages;
    used_pages.push_back(*original_page_);
    for (uint32_t p : available_pages_)
    {
        if (used_pages.size() >= num_pages_needed)
            break;
        used_pages.push_back(p);
    }

    if (used_pages.size() < num_pages_needed)
    {
        bool reupload = false;

        available_pages availablePages = get_available_pages(db_path, *schema.available_pages_ref, page_size);

        for (size_t i = 0; i < extra_available_pages_.size(); i++)
        {
            if (used_pages.size() >= num_pages_needed)
                break;
            uint32_t p = availablePages.available_pages_pointers[i];
            availablePages.available_pages_pointers.erase(availablePages.available_pages_pointers.begin() + i);
            used_pages.push_back(p);
            reupload = true;
        }

        if (reupload)
        {
            set_available_pages_db(db_path, availablePages, page_size);
        }
    }

    size_t next_page = num_pages_in_file(db_path, page_size);
    while (used_pages.size() < num_pages_needed)
    {
        used_pages.push_back(next_page++);
    }

    // Build the header in a temporary buffer
    BitBuffer header;
    header.putU32(static_cast<uint32_t>(used_pages.size() - 1));
    for (size_t i = 1; i < used_pages.size(); i++)
    {
        header.putU32(used_pages[i]);
    }

    // Merge header + payload into a single stream of bytes
    std::vector<uint8_t> final_bytes = header.bytes();
    final_bytes.insert(final_bytes.end(), data.begin(), data.end());

    // Pad to multiple of page_size
    if (final_bytes.size() % page_size != 0)
    {
        size_t pad = page_size - (final_bytes.size() % page_size);
        final_bytes.insert(final_bytes.end(), pad, 0);
    }

    // ---- Write each page individually ----
    if (save)
    {
        std::fstream out(db_path, std::ios::in | std::ios::out | std::ios::binary);
        if (!out)
            throw std::runtime_error("Failed to open database file for writing");

        for (size_t i = 0; i < used_pages.size(); i++)
        {
            uint64_t offset = static_cast<uint64_t>(used_pages[i]) * page_size;
            out.seekp(offset, std::ios::beg);
            out.write(reinterpret_cast<const char *>(&final_bytes[i * page_size]), page_size);

            if (!out)
            {
                throw std::runtime_error("Failed to write page " + std::to_string(used_pages[i]));
            }
        }
        out.flush();
    }

    return used_pages;
}

void ClusteredIndexNode::print() const
{
    std::cout << "ClusteredIndexNode {\n";

    // Print page pointers
    std::cout << "  page_pointers = [";
    for (size_t i = 0; i < page_pointers_.size(); i++)
    {
        std::cout << page_pointers_[i];
        if (i + 1 < page_pointers_.size())
            std::cout << ", ";
    }
    std::cout << "]\n";

    // Print rows
    std::cout << "  rows = [\n";
    for (size_t i = 0; i < items_.size(); i++)
    {
        std::cout << "    Row " << i << ": ";
        items_[i].print(); // delegate to DataRow::print
        std::cout << "\n";
    }
    std::cout << "  ]\n";

    if (original_page_)
    {
        std::cout << "  original_page = " << *original_page_ << "\n";
    }
    else
    {
        std::cout << "  original_page = (none)\n";
    }

    std::cout << available_pages_.size() << std::endl;

    std::cout << "}\n";
}