#pragma once
#include <vector>
#include <cstdint>
#include <optional>
#include "row.hpp"
#include "dbone/bitbuffer.hpp"

class ClusteredIndexNode {
public:
    ClusteredIndexNode() = default;

    // Add a row
    void add_row(DataRow&& row);
    void add_row_at(DataRow&& row, size_t position);
    void add_pointer(uint32_t page_pointer);
    void add_pointer_at(uint32_t page_pointer, size_t position);

    // Set the original page (first page to write to)
    void set_original_page(uint32_t page);

    // Add a list of available pages (in order)
    void set_available_pages(const std::vector<uint32_t>& pages);
    void set_extra_available_pages(const std::vector<uint32_t>& pages);

    std::vector<DataRow>& get_items();

    // Serialize rows into a payload buffer
    BitBuffer to_bits() const;

    // Save clustered index across pages
    // Returns list of page IDs used
    std::vector<uint32_t> save(const std::string &db_path, uint32_t page_size = 4096);

    bool is_leaf() const { return page_pointers_.empty(); }

    void print() const;

private:
    uint32_t min_length_ = 0;
    std::vector<DataRow> items_;
    std::vector<uint32_t> page_pointers_;   // child references

    std::optional<uint32_t> original_page_; // first page
    std::vector<uint32_t> available_pages_; // pool of extra pages
    std::vector<uint32_t> extra_available_pages_;

    uint32_t next_new_page_id_ = 0; // for allocating new pages
};
