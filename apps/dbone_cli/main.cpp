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
#include "dbone/search.hpp"
#include <memory>
#include <iostream>
#include <chrono>
#include <unordered_set>

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

int make_table(uint32_t page_size)
{
    TableSchema s;
    s.table_name = "orders";
    s.columns.emplace_back(std::make_unique<BigIntColumn>("id", /*nullable=*/false, /*pk=*/false, /*uniq=*/true, false, /*default=*/4342596));
    s.columns.emplace_back(std::make_unique<VarCharColumn>("code", 250, /*nullable=*/false, /*pk=*/true, /*uniq=*/true, false, /*default=*/"ABCDEFGH"));
    s.min_length = 128;
    std::string err;
    bool ok = create_table(s, "C:/Users/zakha/Documents/15. Database+/store", &err, page_size);
    if (!ok)
    {
        std::cerr << "create_table failed: " << err << "\n";
        return 1;
    }
}

std::string make_code(size_t id)
{
    std::ostringstream oss;
    oss << "C" << std::setw(7) << std::setfill('0') << id;
    return oss.str();
}

void insert_table(size_t n, uint32_t page_size)
{
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < n; i++)
    {
        dbone::insert::Row row1 = {
            {"id", std::to_string(i)},
            {"code", make_code(i)}};

        auto result = dbone::insert::insert(
            "C:/Users/zakha/Documents/15. Database+/store/table.efdb",
            row1,
            page_size);

        if (!result.ok)
        {
            std::cerr << "Insert failed: " << result.error << "\n";
        }
        else
        {
            // std::cout << "Insert validated successfully!\n";
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Insert took " << duration.count() / 1000.0f << " milloseconds\n";
}

int main(int argc, char **argv)
{

    uint32_t PAGE_SIZE_DEFAULT = 4096;

    // make_table(PAGE_SIZE_DEFAULT);

    // insert_table(10000, PAGE_SIZE_DEFAULT);

    // std::cout << "INSERTED" << std::endl;

    // std::vector<std::unique_ptr<DataType>> primaryKeys;

    // primaryKeys.push_back(std::make_unique<VarCharType>("C0000407", 250));
    // SearchResult result = dbone::search::searchPrimaryKeys("C:/Users/zakha/Documents/15. Database+/store/table.efdb", primaryKeys, PAGE_SIZE_DEFAULT);

    // for (dbone::insert::Row row : result.rows)
    // {
    //     std::cout << "{ ";
    //     for (const auto &[key, value] : row)
    //     {
    //         std::cout << key << ": " << value << ", ";
    //     }
    //     std::cout << "}" << std::endl;
    // }

    // std::cout << result.timeTaken << std::endl;


    // std::cout << "BEFORE" << std::endl;


    std::cout << "STARTING" << std::endl;
    SearchParam param;
    param.columnName = "id";
    param.comparator = Comparator::NonEqual;
    param.compareTo = std::make_unique<BigIntType>(39);
    param.compareTo2 = std::make_unique<BigIntType>(490);
    // param.compareTo = std::make_unique<VarCharType>("C0000132", 250);
    // param.compareTo2 = std::make_unique<VarCharType>("C0000192", 250);
    std::vector<SearchParam> params;
    params.push_back(std::move(param));
    SearchResult result = dbone::search::searchItem("C:/Users/zakha/Documents/15. Database+/store/table.efdb", params, PAGE_SIZE_DEFAULT);
    std::cout << result.rows.size() << std::endl;
    for (dbone::insert::Row row : result.rows)
    {
        std::cout << "{ ";
        for (const auto &[key, value] : row)
        {
            std::cout << key << ": " << value << ", ";
        }
        std::cout << "}" << std::endl;
    }
    std::cout << result.rows.size() << std::endl;
    std::cout << result.timeTaken << std::endl;


    return 0;
}
