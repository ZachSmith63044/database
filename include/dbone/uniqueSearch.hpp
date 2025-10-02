#include <unordered_map>
#include <string>
#include "dbone/schema.hpp"

bool searchUnique(const std::string &db_path, const TableSchema &schema, const std::unordered_map<std::string, std::string>& row, uint32_t page_size);