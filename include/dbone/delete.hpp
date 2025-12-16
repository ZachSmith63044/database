#pragma once
#include "dbone/schema.hpp"
#include <string>
#include <unordered_map>

struct DeleteResult
{
    bool ok;
    std::string error;
};

namespace dbone::delete_db
{

    DeleteResult delete_primary_key(const std::string &db_path, std::unique_ptr<DataType> primary_key, uint32_t page_size);

} // namespace dbone::insert
