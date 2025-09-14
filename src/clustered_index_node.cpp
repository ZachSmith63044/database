#include "dbone/clustered_index_node.hpp"
#include <stdexcept>
#include <algorithm>
#include <iostream>
#include <fstream>
// Add a row
void ClusteredIndexNode::add_row(DataRow&& row)
{
    items_.push_back(std::move(row));
}

// Add a page pointer
void ClusteredIndexNode::add_pointer(uint32_t page_pointer)
{
    page_pointers_.push_back(page_pointer);
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

std::vector<uint32_t> ClusteredIndexNode::save(const std::string &db_path, uint32_t page_size)
{
    std::cout << "SAVE STARTED" << std::endl;
    if (!original_page_) {
        throw std::runtime_error("ClusteredIndexNode::save: original page not set");
    }

    // Build payload of all rows
    BitBuffer payload = to_bits();
    const auto &data = payload.bytes();

    // Start with a guess for number of pages
    size_t num_pages_needed = 1;
    while (true) {
        // Header = 4B count + 4B for each extra page
        size_t header_size = 4 + 4 * (num_pages_needed - 1);
        size_t total_size = header_size + data.size();

        size_t new_num_pages = (total_size + page_size - 1) / page_size;
        if (new_num_pages == num_pages_needed) break;
        num_pages_needed = new_num_pages;
    }

    // Build page allocation order
    std::vector<uint32_t> used_pages;
    used_pages.push_back(*original_page_);
    for (uint32_t p : available_pages_) {
        if (used_pages.size() >= num_pages_needed) break;
        used_pages.push_back(p);
    }
    while (used_pages.size() < num_pages_needed) {
        used_pages.push_back(next_new_page_id_++);
    }

    // Build the header in a temporary buffer
    BitBuffer header;
    header.putU32(static_cast<uint32_t>(used_pages.size() - 1));
    for (size_t i = 1; i < used_pages.size(); i++) {
        header.putU32(used_pages[i]);
    }

    // Merge header + payload into a single stream of bytes
    std::vector<uint8_t> final_bytes = header.bytes();
    final_bytes.insert(final_bytes.end(), data.begin(), data.end());

    // Pad to multiple of page_size
    if (final_bytes.size() % page_size != 0) {
        size_t pad = page_size - (final_bytes.size() % page_size);
        final_bytes.insert(final_bytes.end(), pad, 0);
    }

    // ---- Write each page individually ----
    std::fstream out(db_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open database file for writing");

    for (size_t i = 0; i < used_pages.size(); i++) {
        uint64_t offset = static_cast<uint64_t>(used_pages[i]) * page_size;
        out.seekp(offset, std::ios::beg);
        out.write(reinterpret_cast<const char*>(&final_bytes[i * page_size]), page_size);

        if (!out) {
            throw std::runtime_error("Failed to write page " + std::to_string(used_pages[i]));
        }
    }
    out.flush();

    std::cout << "SAVE COMPLETE: wrote " << used_pages.size() << " pages\n";

    return used_pages;
}




void ClusteredIndexNode::print() const {
    std::cout << "ClusteredIndexNode {\n";

    // Print page pointers
    std::cout << "  page_pointers = [";
    for (size_t i = 0; i < page_pointers_.size(); i++) {
        std::cout << page_pointers_[i];
        if (i + 1 < page_pointers_.size()) std::cout << ", ";
    }
    std::cout << "]\n";

    // Print rows
    std::cout << "  rows = [\n";
    for (size_t i = 0; i < items_.size(); i++) {
        std::cout << "    Row " << i << ": ";
        items_[i].print();  // delegate to DataRow::print
        std::cout << "\n";
    }
    std::cout << "  ]\n";

    if (original_page_) {
        std::cout << "  original_page = " << *original_page_ << "\n";
    } else {
        std::cout << "  original_page = (none)\n";
    }

    std::cout << "}\n";
}