#include "dbone/schema.hpp"
#include "dbone/bitbuffer.hpp"
#include "dbone/constants.hpp"
#include "dbone/index.hpp"
#include "dbone/storage.hpp"
#include "dbone/serialize.hpp"
#include <filesystem>
#include <cstdio>
#include <system_error>
#include <iostream>
#include <fstream>
#include <iomanip> // for std::setw, std::setfill
// using namespace dbone::serialize;
namespace fs = std::filesystem;

// Assumed to exist from your codebase:
// extern void put_u32(FILE *f, uint32_t v);
// extern void put_u16(FILE *f, uint16_t v);
// extern void put_u8(FILE *f, uint8_t v);
// extern void put_bytes(FILE *f, const std::string &s);

extern const uint32_t MAGIC;
extern const uint16_t VERSION;

// --------- Logging ----------
#define LOG(fmt, ...)                                                    \
    do                                                                   \
    {                                                                    \
        std::fprintf(stderr, "[create_table] " fmt "\n", ##__VA_ARGS__); \
        std::fflush(stderr);                                             \
    } while (0)

// Helper: dump a slice of bytes as hex
static void dump_bytes(const std::vector<uint8_t> &buf, size_t off, size_t count, const char *label)
{
    std::cerr << "[dump] " << label << " @off=" << off << " len=" << count << ": ";
    for (size_t i = 0; i < count && off + i < buf.size(); i++)
    {
        std::cerr << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(buf[off + i]) << " ";
    }
    std::cerr << std::dec << "\n";
}

TableSchema read_schema(const std::string &file, uint32_t page_size)
{
    std::cerr << "[read_schema] Opening file: " << file << " page_size=" << page_size << "\n";

    std::ifstream in(file, std::ios::binary);
    if (!in)
        throw std::runtime_error("Failed to open schema file: " + file);

    // --- Step 1: read first page
    std::vector<uint8_t> page(page_size);
    in.read(reinterpret_cast<char *>(page.data()), page.size());
    if (in.gcount() != static_cast<std::streamsize>(page_size))
        throw std::runtime_error("Failed to read first schema page");
    std::cerr << "[read_schema] Read first page OK\n";

    size_t off = 0;

    // page_count (1 byte)
    uint8_t page_count = readU8(page, off);
    std::cerr << "[read_schema] page_count=" << (int)page_count << "\n";

    // page list
    std::vector<uint32_t> page_list;
    for (uint32_t i = 1; i < page_count; ++i)
    {
        uint32_t pg = readU32(page, off);
        page_list.push_back(pg);
    }
    std::cerr << "[read_schema] page_list size=" << page_list.size() << "\n";

    // --- Step 2: figure out payload bytes inside page 0
    uint64_t header_size = 1ull + 4ull * (page_count - 1);
    if (header_size > page_size)
        throw std::runtime_error("Invalid header size > page size");
    size_t payload_in_page0 = page_size - static_cast<size_t>(header_size);
    std::cerr << "[read_schema] header_size=" << header_size
              << " payload_in_page0=" << payload_in_page0 << "\n";

    // gather schema payload
    std::vector<uint8_t> schema_payload;
    schema_payload.insert(schema_payload.end(),
                          page.begin() + header_size,
                          page.begin() + header_size + payload_in_page0);

    std::cerr << "[read_schema] schema_payload after page0=" << schema_payload.size() << " bytes\n";

    // --- Step 3: read additional schema pages if listed
    for (uint32_t pg : page_list)
    {
        std::cerr << "[read_schema] Reading schema page " << pg << "\n";
        in.seekg(static_cast<std::streamoff>(pg) * page_size, std::ios::beg);
        std::vector<uint8_t> buf(page_size);
        in.read(reinterpret_cast<char *>(buf.data()), buf.size());
        if (in.gcount() != static_cast<std::streamsize>(page_size))
            throw std::runtime_error("Failed to read schema page " + std::to_string(pg));
        schema_payload.insert(schema_payload.end(), buf.begin(), buf.end());
    }
    std::cerr << "[read_schema] Final schema_payload=" << schema_payload.size() << " bytes\n";

    // --- Step 4: parse schema_payload
    off = 0;
    uint32_t data_root_page = readU32(schema_payload, off);
    std::cerr << "[read_schema] data_root_page=" << data_root_page << "\n";

    dump_bytes(schema_payload, off, 16, "start of schema_body");

    uint32_t magic = readU32(schema_payload, off);
    uint16_t version = readU16(schema_payload, off);
    std::cerr << "[read_schema] MAGIC=0x" << std::hex << magic
              << " VERSION=" << std::dec << version << "\n";

    dump_bytes(schema_payload, off, 16, "before table_name");
    std::string table_name = readString(schema_payload, off);
    std::cerr << "[read_schema] table_name=" << table_name << "\n";

    uint16_t col_count = readU16(schema_payload, off);
    std::cerr << "[read_schema] col_count=" << col_count << "\n";

    TableSchema schema;
    schema.table_name = table_name;
    schema.clustered_page_ref = data_root_page;

    for (uint16_t i = 0; i < col_count; ++i)
    {
        std::cerr << "[read_schema] --- column " << i << " ---\n";

        dump_bytes(schema_payload, off, 16, "before col_name");
        std::string col_name = readString(schema_payload, off);
        std::cerr << "[read_schema] col_name=" << col_name << "\n";

        uint8_t packed = readU8(schema_payload, off);
        std::cerr << "[read_schema] packed=0x" << std::hex << (int)packed << std::dec << "\n";

        bool nullable = (packed & (1u << 7)) != 0;
        bool primaryKey = (packed & (1u << 6)) != 0;
        bool unique = (packed & (1u << 5)) != 0;
        uint8_t type_id = packed & 0x1F;
        std::cerr << " flags: nullable=" << nullable
                  << " pk=" << primaryKey
                  << " uniq=" << unique
                  << " type=" << (int)type_id << "\n";

        if (type_id == 1)
        { // BIGINT
            dump_bytes(schema_payload, off, 8, "BIGINT default bytes");
            if (off + 8 > schema_payload.size())
                throw std::runtime_error("BIGINT default out of bounds");
            int64_t def = 0;
            for (int b = 0; b < 8; ++b)
            {
                def |= static_cast<int64_t>(schema_payload[off + b]) << (8 * b);
            }
            off += 8;
            std::cerr << "[read_schema] BIGINT default=" << def << "\n";
            schema.columns.emplace_back(
                std::make_unique<BigIntColumn>(col_name, nullable, primaryKey, unique, def));
        }
        else if (type_id == 2)
        { // CHAR(N)
            uint32_t len = readU32(schema_payload, off);
            std::cerr << "[read_schema] CHAR length=" << len << "\n";
            dump_bytes(schema_payload, off, len, "CHAR default bytes");
            if (off + len > schema_payload.size())
                throw std::runtime_error("CHAR default out of bounds");
            std::string def(reinterpret_cast<const char *>(&schema_payload[off]), len);
            off += len;
            std::cerr << "[read_schema] CHAR default='" << def << "'\n";
            schema.columns.emplace_back(
                std::make_unique<CharColumn>(col_name, len, nullable, primaryKey, unique, def));
        }
        else
        {
            throw std::runtime_error("Unknown column type " + std::to_string(type_id));
        }
    }
    schema.min_length = readU32(schema_payload, off);
    
    std::cout << "[read_schema] data_root_inside=" << *schema.clustered_page_ref << std::endl;
    std::cerr << "[read_schema] DONE parsing\n";
    return schema;
}

bool create_table(const TableSchema &s,
                  const std::string &path,
                  std::string *err,
                  std::uint32_t page_size)
{
    namespace fs = std::filesystem;

    LOG("ENTER create_table: path='%s', page_size=%u", path.c_str(), page_size);

    if (page_size == 0)
    {
        if (err)
            *err = "page_size must be > 0";
        LOG("ERROR: page_size == 0");
        return false;
    }

    fs::path out_path(path);
    if (fs::is_directory(out_path))
    {
        out_path /= "table.efdb";
        LOG("path is a directory; writing to '%s'", out_path.string().c_str());
    }
    else
    {
        LOG("path is a file; writing to '%s'", out_path.string().c_str());
    }

    if (!out_path.parent_path().empty())
    {
        std::error_code ec;
        fs::create_directories(out_path.parent_path(), ec);
        if (ec)
        {
            if (err)
                *err = "failed to create directory: " + out_path.parent_path().string();
            LOG("ERROR: create_directories('%s') failed: %s",
                out_path.parent_path().string().c_str(), ec.message().c_str());
            return false;
        }
    }

    // --- 1) Serialize schema into bit buffer
    LOG("serialize schema: table='%s', cols=%zu", s.table_name.c_str(), s.columns.size());

    BitBuffer buf;
    buf.putU32(MAGIC);
    buf.putU16(VERSION);
    buf.putString(s.table_name);

    // number of columns (u16)
    buf.putU16(static_cast<uint16_t>(s.columns.size()));

    // each column serializes itself
    for (const auto &col : s.columns)
    {
        col->to_bits(buf);
    }

    // add new u32 at end of schema
    buf.putU32(s.min_length);

    const std::vector<uint8_t> &schema_body = buf.bytes();
    LOG("schema_body size=%zu bytes", schema_body.size());
    LOG("computing page count: need=%zu (schema_body + 4)", schema_body.size() + sizeof(uint32_t));

    LOG("schema_body size=%zu bytes", schema_body.size());

    // --- 2) Compute how many pages are required
    auto capacity_for_pages = [page_size](uint32_t N) -> int64_t
    {
        int64_t header = 1 + 4LL * (N > 0 ? (N - 1) : 0);
        if (header > page_size)
            return -1;
        return static_cast<int64_t>(N) * page_size - header;
    };

    uint32_t page_count = 1;
    while (true)
    {
        int64_t cap = capacity_for_pages(page_count);
        LOG("try page_count=%u -> capacity=%lld", page_count, (long long)cap);
        if (cap < 0)
        {
            if (err)
                *err = "header exceeds page size";
            LOG("ERROR: header exceeds page size");
            return false;
        }
        uint64_t need = schema_body.size() + sizeof(uint32_t);
        if (static_cast<uint64_t>(cap) >= need)
            break;
        page_count++;
        if (page_count > 255)
        {
            if (err)
                *err = "schema too large: exceeds 255 pages";
            LOG("ERROR: schema too large (>255 pages)");
            return false;
        }
    }

    const uint32_t data_root_page = page_count;
    const uint32_t total_pages = page_count + 1;
    LOG("decided: schema pages=%u, data_root_page=%u, total_pages=%u",
        page_count, data_root_page, total_pages);

    // --- 3) Payload: root page pointer + schema body
    std::vector<uint8_t> payload;
    payload.reserve(schema_body.size() + sizeof(uint32_t));
    payload.push_back(uint8_t(data_root_page & 0xFF));
    payload.push_back(uint8_t((data_root_page >> 8) & 0xFF));
    payload.push_back(uint8_t((data_root_page >> 16) & 0xFF));
    payload.push_back(uint8_t((data_root_page >> 24) & 0xFF));
    payload.insert(payload.end(), schema_body.begin(), schema_body.end());
    const uint64_t payload_size = payload.size();
    LOG("payload size=%llu bytes", (unsigned long long)payload_size);

    // --- 4) Create file and size it
    FILE *f = std::fopen(out_path.string().c_str(), "wb+");
    if (!f)
    {
        if (err)
            *err = "failed to open: " + out_path.string();
        LOG("ERROR: fopen('%s') failed (errno=%d)", out_path.string().c_str(), errno);
        return false;
    }
    LOG("opened file");

    const uint64_t file_size = static_cast<uint64_t>(total_pages) * page_size;
    LOG("sizing file to %llu bytes (%u pages @ %u)", (unsigned long long)file_size, total_pages, page_size);

    if (dbone::storage::seek64(f, static_cast<long long>(file_size - 1), SEEK_SET) != 0 || std::fputc(0, f) == EOF)
    {
        if (err)
            *err = "failed to extend file";
        LOG("ERROR: extending file failed (errno=%d)", errno);
        std::fclose(f);
        return false;
    }

    // --- 5) Write header
    uint8_t page_count_u8 = static_cast<uint8_t>(page_count);
    if (!dbone::storage::write_at64(f, 0, &page_count_u8, 1))
    {
        if (err)
            *err = "failed writing page_count";
        LOG("ERROR: writing page_count");
        std::fclose(f);
        return false;
    }
    LOG("wrote header: page_count=%u", page_count);

    if (page_count > 1)
    {
        std::vector<uint8_t> list;
        list.reserve(4u * (page_count - 1));
        for (uint32_t i = 1; i < page_count; ++i)
        {
            list.push_back(uint8_t(i & 0xFF));
            list.push_back(uint8_t((i >> 8) & 0xFF));
            list.push_back(uint8_t((i >> 16) & 0xFF));
            list.push_back(uint8_t((i >> 24) & 0xFF));
        }
        if (!dbone::storage::write_at64(f, 1, list.data(), list.size()))
        {
            if (err)
                *err = "failed writing page list";
            LOG("ERROR: writing page list (bytes=%zu)", list.size());
            std::fclose(f);
            return false;
        }
        LOG("wrote page list: entries=%u, bytes=%zu", page_count - 1, list.size());
    }

    // --- 6) Write payload
    const uint64_t header_size = 1ull + 4ull * (page_count - 1);
    LOG("header_size=%llu", (unsigned long long)header_size);

    uint64_t remaining = payload_size;
    uint64_t src_off = 0;

    {
        uint64_t page0_cap = page_size - header_size;
        uint64_t to_write = std::min<uint64_t>(remaining, page0_cap);
        LOG("page0_cap=%llu, writing %llu bytes at off=%llu",
            (unsigned long long)page0_cap, (unsigned long long)to_write, (unsigned long long)header_size);

        if (to_write > 0)
        {
            if (!dbone::storage::write_at64(f, header_size, payload.data() + src_off, static_cast<size_t>(to_write)))
            {
                if (err)
                    *err = "failed writing payload to page 0";
                LOG("ERROR: payload page0 write");
                std::fclose(f);
                return false;
            }
            remaining -= to_write;
            src_off += to_write;
        }
    }

    for (uint32_t p = 1; p < page_count && remaining > 0; ++p)
    {
        uint64_t page_off = static_cast<uint64_t>(p) * page_size;
        uint64_t to_write = std::min<uint64_t>(remaining, page_size);
        LOG("writing page %u: off=%llu, bytes=%llu", p,
            (unsigned long long)page_off, (unsigned long long)to_write);

        if (!dbone::storage::write_at64(f, page_off, payload.data() + src_off, static_cast<size_t>(to_write)))
        {
            if (err)
                *err = "failed writing payload to page " + std::to_string(p);
            LOG("ERROR: payload write page %u", p);
            std::fclose(f);
            return false;
        }
        remaining -= to_write;
        src_off += to_write;
    }
    LOG("payload write done, remaining=%llu", (unsigned long long)remaining);

    // --- 7) Initialize clustered index root page
    LOG("init clustered index at page %u", data_root_page);
    if (!dbone::index::add_empty_clustered_index(f, data_root_page, page_size))
    {
        if (err)
            *err = "failed writing clustered index root page";
        LOG("ERROR: clustered index init");
        std::fclose(f);
        return false;
    }

    std::fclose(f);
    LOG("file closed");

    // Verify on-disk size
    try
    {
        auto sz = fs::file_size(out_path);
        LOG("final file_size=%llu (expected %llu)",
            (unsigned long long)sz, (unsigned long long)file_size);
        if (sz != file_size)
        {
            if (err)
                *err = "size mismatch: expected " + std::to_string(file_size) +
                       " got " + std::to_string(sz);
            LOG("ERROR: size mismatch!");
            return false;
        }
    }
    catch (const std::exception &e)
    {
        LOG("WARNING: file_size() exception: %s", e.what());
    }

    LOG("SUCCESS");
    return true;
}
