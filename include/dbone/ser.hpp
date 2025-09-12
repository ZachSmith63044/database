#pragma once
#include <cstdint>
#include <cstdio>
#include <string>

namespace dbone::ser {

// public constants
inline constexpr uint32_t MAGIC   = 0xDB5C43A1u;
inline constexpr uint16_t VERSION = 1u;

// binary write helpers (little-endian)
void put_u32(FILE* f, uint32_t v);
void put_u16(FILE* f, uint16_t v);
void put_u8 (FILE* f, uint8_t v);
void put_bytes(FILE* f, const std::string& s);

} // namespace dbone::ser
