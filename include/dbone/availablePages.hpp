#include <vector>
#include <string>

struct available_pages {
    std::vector<uint32_t> page_pointers; // ordered including page 0
    std::vector<uint32_t> available_pages_pointers;
};

available_pages get_available_pages(const std::string &file_name, uint32_t available_pages_ref, uint32_t page_size=4096);
bool  set_available_pages(const std::string &file_name, available_pages available_pages_info, uint32_t page_size=4096);