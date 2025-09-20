#include "dbone/secondary_index_node.hpp"
#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <algorithm>
#include "dbone/availablePages.hpp"
#include <dbone/serialize.hpp>

using std::uint32_t;
using std::uint64_t;

// --------- mutators ----------
void SecondaryIndexNode::add_entry_at(IndexEntry&& e, size_t pos) {
    if (pos > entries_.size()) throw std::out_of_range("add_entry_at");
    entries_.insert(entries_.begin()+pos, std::move(e));
}
void SecondaryIndexNode::add_pointer_at(uint32_t p, size_t pos) {
    if (pos > page_pointers_.size()) throw std::out_of_range("add_pointer_at");
    page_pointers_.insert(page_pointers_.begin()+pos, p);
}
void SecondaryIndexNode::set_pointer_at(uint32_t p, size_t pos) {
    if (pos >= page_pointers_.size()) throw std::out_of_range("set_pointer_at");
    page_pointers_[pos] = p;
}
void SecondaryIndexNode::set_original_page(uint32_t page) {
    original_page_ = page;
    if (page >= next_new_page_id_) next_new_page_id_ = page + 1;
}
void SecondaryIndexNode::set_available_pages(const std::vector<uint32_t>& pages) {
    available_pages_ = pages;
    for (auto p : pages) if (p >= next_new_page_id_) next_new_page_id_ = p + 1;
}
void SecondaryIndexNode::set_extra_available_pages(const std::vector<uint32_t>& pages) {
    extra_available_pages_ = pages;
}

// --------- file util ----------
size_t SecondaryIndexNode::num_pages_in_file(const std::string& path, size_t page_size) {
    namespace fs = std::filesystem;
    auto file_size = fs::file_size(path);
    return file_size / page_size;
}

// --------- load ----------
SecondaryIndexNode SecondaryIndexNode::load(const std::string& db_path,
                                            uint32_t page_num,
                                            const TableSchema& schema,
                                            const Column& indexed_col,
                                            const Column& pk_col,
                                            uint32_t page_size) {
    std::ifstream in(db_path, std::ios::binary);
    if (!in) throw std::runtime_error("SecondaryIndexNode::load: open failed");

    // read root page
    const uint64_t offset = uint64_t(page_num) * page_size;
    in.seekg(offset, std::ios::beg);
    if (!in) throw std::runtime_error("SecondaryIndexNode::load: seek root failed");

    std::vector<uint8_t> root(page_size);
    in.read(reinterpret_cast<char*>(root.data()), root.size());
    if (!in) throw std::runtime_error("SecondaryIndexNode::load: read root failed");

    size_t off = 0;
    uint32_t page_count = readU32(root, off); // number of extra pages after root

    // collect page list
    std::vector<uint32_t> page_list;
    page_list.reserve(page_count);
    for (uint32_t i = 0; i < page_count; ++i) {
        page_list.push_back(readU32(root, off));
    }

    // header bytes consumed: 4 + 4*page_count
    const size_t header_size = 4u + 4u * page_count;
    if (header_size > page_size) throw std::runtime_error("SecondaryIndexNode::load: header too large");

    // gather full payload: tail of root + all subsequent pages
    std::vector<uint8_t> full_payload;
    if (header_size < root.size()) {
        full_payload.insert(full_payload.end(), root.begin() + header_size, root.end());
    }
    for (auto pg : page_list) {
        std::vector<uint8_t> buf(page_size);
        const uint64_t pg_off = uint64_t(pg) * page_size;
        in.seekg(pg_off, std::ios::beg);
        if (!in) throw std::runtime_error("SecondaryIndexNode::load: seek page failed");
        in.read(reinterpret_cast<char*>(buf.data()), buf.size());
        if (!in) throw std::runtime_error("SecondaryIndexNode::load: read page failed");
        full_payload.insert(full_payload.end(), buf.begin(), buf.end());
    }

    size_t ref = 0;

    // payload format:
    // U32 nEntries
    // U32 left_ptr
    // repeat nEntries times:
    //   <indexed value>         (DataType via indexed_col)
    //   U32 key_count
    //   repeat key_count times:
    //       <pk value>          (DataType via pk_col)
    //   U32 right_ptr

    const uint32_t nEntries = readU32(full_payload, ref);

    SecondaryIndexNode node;
    node.set_available_pages(page_list);
    node.set_original_page(page_num);

    // leftmost pointer (exists even if leaf: can be 0)
    node.add_pointer(readU32(full_payload, ref));

    node.entries_.reserve(nEntries);
    for (uint32_t i = 0; i < nEntries; ++i) {
        auto value = indexed_col.from_bits(full_payload, ref);

        uint32_t key_count = readU32(full_payload, ref);
        IndexEntry entry(std::move(value));
        entry.primary_keys.reserve(key_count);
        for (uint32_t k = 0; k < key_count; ++k) {
            entry.primary_keys.push_back(pk_col.from_bits(full_payload, ref));
        }

        node.entries_.push_back(std::move(entry));

        // right child pointer after each entry
        node.add_pointer(readU32(full_payload, ref));
    }

    return node;
}

// --------- to_bits ----------
BitBuffer SecondaryIndexNode::to_bits(const Column& indexed_col, const Column& pk_col) const {
    (void)indexed_col; (void)pk_col; // not needed for writing: DataType knows how to serialize itself
    BitBuffer buf;

    // nEntries
    buf.putU32(static_cast<uint32_t>(entries_.size()));

    // leftmost pointer
    uint32_t leftp = page_pointers_.empty() ? 0 : page_pointers_[0];
    buf.putU32(leftp);

    // entries
    for (size_t i = 0; i < entries_.size(); ++i) {
        // value
        entries_[i].value->to_bits(buf);

        // postings length
        buf.putU32(static_cast<uint32_t>(entries_[i].primary_keys.size()));

        // postings
        for (const auto& pk : entries_[i].primary_keys) {
            pk->to_bits(buf);
        }

        // right pointer for this entry
        uint32_t rp = (page_pointers_.size() == entries_.size() + 1) ? page_pointers_[i+1] : 0;
        buf.putU32(rp);
    }

    return buf;
}

// --------- save ----------
std::vector<uint32_t> SecondaryIndexNode::save(const std::string& db_path,
                                               const TableSchema& schema,
                                               uint32_t page_size,
                                               bool do_save) const {
    if (!original_page_) throw std::runtime_error("SecondaryIndexNode::save: original page not set");

    BitBuffer payload = to_bits(*schema.columns.front(), *schema.columns.front()); // columns not used on write
    const auto& data = payload.bytes();

    // compute pages needed (same approach as your ClusteredIndexNode)
    size_t num_pages_needed = 1;
    while (true) {
        size_t header_size = 4 + 4 * (num_pages_needed - 1);
        size_t total_size  = header_size + data.size();
        size_t new_n = (total_size + page_size - 1) / page_size;
        if (new_n == num_pages_needed) break;
        num_pages_needed = new_n;
    }

    std::vector<uint32_t> used;
    used.push_back(*original_page_);
    for (uint32_t p : available_pages_) {
        if (used.size() >= num_pages_needed) break;
        used.push_back(p);
    }

    if (used.size() < num_pages_needed) {
        bool reupload = false;

        available_pages avail = get_available_pages(db_path, *schema.available_pages_ref, page_size);
        for (size_t i = 0; i < avail.available_pages_pointers.size() && used.size() < num_pages_needed; ++i) {
            uint32_t p = avail.available_pages_pointers[i];
            avail.available_pages_pointers.erase(avail.available_pages_pointers.begin() + i);
            used.push_back(p);
            reupload = true;
        }
        if (reupload) set_available_pages_db(db_path, avail, page_size);
    }

    size_t next_page = num_pages_in_file(db_path, page_size);
    while (used.size() < num_pages_needed) used.push_back(static_cast<uint32_t>(next_page++));

    // header: U32 count_of_extra_pages, then the page ids (excluding root)
    BitBuffer header;
    header.putU32(static_cast<uint32_t>(used.size() - 1));
    for (size_t i = 1; i < used.size(); ++i) header.putU32(used[i]);

    std::vector<uint8_t> final_bytes = header.bytes();
    final_bytes.insert(final_bytes.end(), data.begin(), data.end());

    if (final_bytes.size() % page_size != 0) {
        size_t pad = page_size - (final_bytes.size() % page_size);
        final_bytes.insert(final_bytes.end(), pad, 0);
    }

    if (do_save) {
        std::fstream out(db_path, std::ios::in | std::ios::out | std::ios::binary);
        if (!out) throw std::runtime_error("SecondaryIndexNode::save: open for write failed");

        for (size_t i = 0; i < used.size(); ++i) {
            uint64_t off = uint64_t(used[i]) * page_size;
            out.seekp(off, std::ios::beg);
            out.write(reinterpret_cast<const char*>(&final_bytes[i * page_size]), page_size);
            if (!out) throw std::runtime_error("SecondaryIndexNode::save: write page failed");
        }
        out.flush();
    }

    return used;
}
