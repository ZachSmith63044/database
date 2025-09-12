#include "dbone/index.hpp"
#include "dbone/storage.hpp"
#include <vector>
#include <cstdint>

namespace dbone::index {

bool add_empty_clustered_index(FILE *f, uint32_t page_id, uint32_t page_size)
{
    const uint64_t page_off = static_cast<uint64_t>(page_id) * page_size;
    std::vector<uint8_t> buf(page_size, 0);

    // layout: [u32 list_len=0][u32 row_count=0]
    for (int i = 0; i < 8; i++) buf[i] = 0;

    return dbone::storage::write_at64(f, page_off, buf.data(), buf.size());
}

} // namespace dbone::index
