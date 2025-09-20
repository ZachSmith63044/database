#pragma once
#include <vector>
#include <memory>
#include <cstdint>
#include "dbone/bitbuffer.hpp"
#include "dbone/columns/column.hpp"     // Column, ColumnType, DataType
#include "row.hpp"                      // for TableSchema (you have it there)

struct IndexEntry {
    std::unique_ptr<DataType> value;                        // indexed value (e.g., "London")
    std::vector<std::unique_ptr<DataType>> primary_keys;    // postings list of PKs

    IndexEntry() = default;
    explicit IndexEntry(std::unique_ptr<DataType> v) : value(std::move(v)) {}

    IndexEntry(const IndexEntry& other) {
        value = other.value ? other.value->clone() : nullptr;
        primary_keys.reserve(other.primary_keys.size());
        for (const auto& k : other.primary_keys) primary_keys.push_back(k->clone());
    }

    IndexEntry& operator=(const IndexEntry& other) {
        if (this == &other) return *this;
        value = other.value ? other.value->clone() : nullptr;
        primary_keys.clear(); primary_keys.reserve(other.primary_keys.size());
        for (const auto& k : other.primary_keys) primary_keys.push_back(k->clone());
        return *this;
    }

    IndexEntry(IndexEntry&&) noexcept = default;
    IndexEntry& operator=(IndexEntry&&) noexcept = default;
};

class SecondaryIndexNode {
public:
    SecondaryIndexNode() = default;

    // Load an index node rooted at page_num.
    // Pass the Column describing the indexed column (for decoding values),
    // and the PK column (for decoding PK values).
    static SecondaryIndexNode load(const std::string& db_path,
                                   uint32_t page_num,
                                   const TableSchema& schema,
                                   const Column& indexed_col,
                                   const Column& pk_col,
                                   uint32_t page_size = 4096);

    // Save this node (and any overflow pages), returns pages used (root first).
    std::vector<uint32_t> save(const std::string& db_path,
                               const TableSchema& schema,
                               uint32_t page_size = 4096,
                               bool do_save = true) const;

    // Serialize whole node payload (not including the multi-page header).
    BitBuffer to_bits(const Column& indexed_col, const Column& pk_col) const;

    // Basic accessors/mutators
    std::vector<IndexEntry>&       entries()       { return entries_; }
    const std::vector<IndexEntry>& entries() const { return entries_; }

    std::vector<uint32_t>&       page_pointers()       { return page_pointers_; }
    const std::vector<uint32_t>& page_pointers() const { return page_pointers_; }

    bool is_leaf() const { return page_pointers_.empty(); }

    void add_entry(IndexEntry&& e) { entries_.push_back(std::move(e)); }
    void add_entry_at(IndexEntry&& e, size_t pos);
    void add_pointer(uint32_t p) { page_pointers_.push_back(p); }
    void add_pointer_at(uint32_t p, size_t pos);
    void set_pointer_at(uint32_t p, size_t pos);

    void set_original_page(uint32_t page);
    std::optional<uint32_t> original_page() const { return original_page_; }

    void set_available_pages(const std::vector<uint32_t>& pages);
    void set_extra_available_pages(const std::vector<uint32_t>& pages);

    // std::vector<IndexEntry> get_entrues() const { return entries_; }
    std::vector<uint32_t> get_available_pages_index() const { return available_pages_; }

private:
    // disk helpers (same pattern as ClusteredIndexNode)
    static size_t num_pages_in_file(const std::string& path, size_t page_size);

private:
    std::vector<IndexEntry> entries_;
    std::vector<uint32_t> page_pointers_;                // size == entries_.size()+1 for internal, ==1 for leaf
    std::optional<uint32_t> original_page_;
    std::vector<uint32_t> available_pages_;
    std::vector<uint32_t> extra_available_pages_;
    uint32_t next_new_page_id_ = 0;
};
