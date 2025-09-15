#include "dbone/availablePages.hpp"
#include <iostream>
#include <fstream>
#include "dbone/serialize.hpp"
#include <iomanip>

struct PagesBlob
{
    std::vector<uint8_t> payload;
    std::vector<uint32_t> readPages;
};

PagesBlob load_available_pages_blob(const std::string &file_name,
                                    uint32_t available_pages_ref,
                                    uint32_t page_size)
{
    std::ifstream in(file_name, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open file: " + file_name);
    }

    const uint64_t root_offset = static_cast<uint64_t>(available_pages_ref) * page_size;
    in.seekg(root_offset, std::ios::beg);

    std::vector<uint8_t> root(page_size);
    if (!in.read(reinterpret_cast<char*>(root.data()), root.size())) {
        throw std::runtime_error("Failed to read root page");
    }

    size_t off = 0;
    uint32_t page_count = readU32(root, off);

    PagesBlob result;
    result.readPages.reserve(page_count);

    for (uint32_t i = 0; i < page_count; i++) {
        uint32_t pid = readU32(root, off);
        result.readPages.push_back(pid);
    }

    // Root payload = everything after the header (count + page IDs)
    size_t root_payload_offset = off;
    size_t root_payload_size   = page_size - root_payload_offset;
    result.payload.insert(result.payload.end(),
                          root.begin() + root_payload_offset,
                          root.end());

    // Append each referenced page in full
    for (uint32_t page_id : result.readPages) {
        const uint64_t page_offset = static_cast<uint64_t>(page_id) * page_size;
        in.seekg(page_offset, std::ios::beg);

        std::vector<uint8_t> page(page_size);
        if (!in.read(reinterpret_cast<char*>(page.data()), page.size())) {
            throw std::runtime_error("Failed to read page id " + std::to_string(page_id));
        }

        result.payload.insert(result.payload.end(), page.begin(), page.end());
    }

    return result;
}


available_pages get_available_pages(const std::string &file_name, uint32_t available_pages_ref, uint32_t page_size)
{
    available_pages availablePages;

    PagesBlob returnVal = load_available_pages_blob(file_name, available_pages_ref, page_size);
    std::vector<uint8_t> payload = returnVal.payload;
    std::vector<uint32_t> readPages = returnVal.readPages;
    readPages.insert(readPages.begin(), available_pages_ref);

    availablePages.read_from_page_pointers = readPages;

    std::cout << payload.size() << std::endl;

    size_t off = 0;
    uint32_t legnthAvailablePages = readU32(payload, off);
    std::vector<uint32_t> availablePagesVect;
    for (uint32_t i = 0; i < legnthAvailablePages; i++)
    {
        availablePagesVect.push_back(readU32(payload, off));
    }

    availablePages.available_pages_pointers = availablePagesVect;

    return availablePages;
}

bool set_available_pages_db(const std::string &file_name,
                         const available_pages &available_pages_info,
                         uint32_t page_size)
{
    std::fstream out(file_name, std::ios::in | std::ios::out | std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to open file: " + file_name);
    }

    // Root page id is always the first one
    if (available_pages_info.read_from_page_pointers.empty()) {
        throw std::runtime_error("read_from_page_pointers is empty");
    }
    uint32_t root_page_id = available_pages_info.read_from_page_pointers[0];

    // === Step 1: Build root header ===
    const uint32_t count = static_cast<uint32_t>(available_pages_info.read_from_page_pointers.size() - 1);

    std::vector<uint8_t> root_page(page_size, 0);
    size_t cursor = 0;

    auto writeU32 = [&](uint32_t v) {
        root_page[cursor++] = static_cast<uint8_t>(v & 0xFF);
        root_page[cursor++] = static_cast<uint8_t>((v >> 8) & 0xFF);
        root_page[cursor++] = static_cast<uint8_t>((v >> 16) & 0xFF);
        root_page[cursor++] = static_cast<uint8_t>((v >> 24) & 0xFF);
    };

    writeU32(count);
    for (size_t i = 1; i < available_pages_info.read_from_page_pointers.size(); i++) {
        writeU32(available_pages_info.read_from_page_pointers[i]);
    }

    // === Step 2: Append available pages list ===
    writeU32(static_cast<uint32_t>(available_pages_info.available_pages_pointers.size()));
    for (uint32_t v : available_pages_info.available_pages_pointers) {
        writeU32(v);
    }

    // === Step 3: Write root page ===
    uint64_t offset = static_cast<uint64_t>(root_page_id) * page_size;
    out.seekp(offset, std::ios::beg);
    out.write(reinterpret_cast<const char*>(root_page.data()), root_page.size());
    if (!out) {
        throw std::runtime_error("Failed to write root page");
    }

    out.flush();
    return true;
}
