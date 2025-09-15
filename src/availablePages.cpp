#include "dbone/availablePages.hpp"
#include <iostream>
#include <fstream>
#include "dbone/serialize.hpp"

available_pages get_available_pages(const std::string &file_name, uint32_t available_pages_ref, uint32_t page_size)
{
    available_pages availablePages;

    std::ifstream in(file_name, std::ios::binary);
    uint64_t offset = static_cast<uint64_t>(available_pages_ref) * page_size;
    in.seekg(offset, std::ios::beg);
    std::vector<uint8_t> root(page_size);
    in.read(reinterpret_cast<char *>(root.data()), root.size());
    size_t off = 0;
    uint32_t page_count = readU32(root, off);
    std::vector<uint32_t> readPages;
    for (uint32_t i = 0; i < page_count; i++)
    {
        readPages.push_back(readU32(root, off));
    }

    return availablePages;
}

bool set_available_pages(const std::string &file_name, available_pages available_pages_info, uint32_t page_size)
{
    return true;
}