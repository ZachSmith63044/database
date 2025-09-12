#pragma once
#include <cstdint>
#include <cstdio>
#include <string>

namespace dbone::index {

// Initialize an empty clustered index page.
// Layout: [u32 list_len=0][u32 row_count=0], rest zero.
bool add_empty_clustered_index(FILE *f, uint32_t page_id, uint32_t page_size);

} // namespace dbone::index
