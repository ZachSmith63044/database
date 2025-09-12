#include "dbone/schema.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <cstdio>
#include <cstdint>
#include "dbone/columns/column.hpp"
#include "dbone/insert.hpp"
#include <memory>
#include <iostream>

namespace fs = std::filesystem;

static bool read_page_count_and_size(const std::string &file, uint8_t &page_count, std::uintmax_t &fsize)
{
    std::ifstream in(file, std::ios::binary);
    if (!in)
        return false;
    char c{};
    in.read(&c, 1);
    if (!in)
        return false;
    page_count = static_cast<uint8_t>(c);
    in.close();

    std::error_code ec;
    fsize = fs::file_size(file, ec);
    return !ec;
}

int main(int argc, char **argv)
{
    try
    {
        // std::string outPath = (argc > 1) ? argv[1] : std::string("schema_out");

        // // Build a sample schema
        // TableSchema s;
        // s.table_name = "orders";
        // s.columns.emplace_back(std::make_unique<BigIntColumn>("id", /*nullable=*/false, /*pk=*/true, /*uniq=*/true, /*default=*/4342596));
        // s.columns.emplace_back(std::make_unique<CharColumn>("code", 8, /*nullable=*/false, /*pk=*/false, /*uniq=*/true, /*default=*/"ABCDEFGH"));
        uint32_t PAGE_SIZE_DEFAULT = 4096;

        // Example insert
        dbone::insert::Row row1 = {
            {"id", "123"},
            {"code", "ABCDEFGH"},
            {"imnotvar", "teeheehee"}};

        auto result = dbone::insert::insert("C:/Users/zakha/Documents/15. Database+/store/table.efdb", row1, PAGE_SIZE_DEFAULT);
        if (!result.ok)
        {
            std::cerr << "Insert failed: " << result.error << "\n";
        }
        else
        {
            std::cout << "Insert validated successfully!\n";
        }

        // std::string err;
        // bool ok = create_table(s, "C:/Users/zakha/Documents/15. Database+/store", &err, PAGE_SIZE_DEFAULT);
        // if (!ok)
        // {
        //     std::cerr << "create_table failed: " << err << "\n";
        //     return 1;
        // }

        // // Determine actual file path (if directory, writer should have created "table.efdb" in it)
        // fs::path outp(outPath);
        // if (fs::is_directory(outp))
        //     outp /= "table.efdb";
        // std::string file = outp.string();

        // // Read back header info for a sanity check
        // uint8_t page_count = 0;
        // std::uintmax_t file_size = 0;
        // if (!read_page_count_and_size(file, page_count, file_size))
        // {
        //     std::cerr << "Failed to read back page_count or file size from: " << file << "\n";
        //     return 2;
        // }

        // std::cout << "Schema written to: " << file << "\n";
        // std::cout << "Page size (expected): " << PAGE_SIZE_DEFAULT << " bytes\n";
        // std::cout << "Page count (from file): " << static_cast<int>(page_count) << "\n";
        // std::cout << "File size: " << file_size << " bytes\n";

        // const std::uint64_t expected_size = static_cast<std::uint64_t>(page_count) * PAGE_SIZE_DEFAULT;
        // if (file_size != expected_size)
        // {
        //     std::cerr << "WARNING: file size != page_count * PAGE_SIZE_DEFAULT ("
        //               << file_size << " != " << expected_size << ")\n";
        // }
        // else
        // {
        //     std::cout << "OK: file size matches page_count × 4096\n";
        // }

        return 0;
    }
    catch (const std::exception &ex)
    {
        std::cerr << "Exception: " << ex.what() << "\n";
        return 3;
    }
}
