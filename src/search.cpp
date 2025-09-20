#include "dbone/search.hpp"
#include "dbone/clustered_index_node.hpp"
#include <chrono>

SearchResult searchMultiPrimaryKeys(
    const std::string &db_path,
    const TableSchema &schema,
    uint32_t currentPage,
    std::vector<std::unique_ptr<DataType>> &primaryKeys,
    uint32_t page_size,
    size_t &offset)
{
    size_t primaryColumnIndex = static_cast<size_t>(-1);
    for (size_t i = 0; i < schema.columns.size(); i++)
    {
        if (schema.columns[i]->primaryKey())
        {
            primaryColumnIndex = i;
            break;
        }
    }
    if (primaryColumnIndex == static_cast<size_t>(-1))
    {
        throw std::runtime_error("Can't find primary column index. [searchPrimaryKeys]");
    }

    ClusteredIndexNode clusteredIndexNode =
        ClusteredIndexNode::load(db_path, currentPage, schema, page_size);
    std::vector<DataRow> &items = clusteredIndexNode.get_items();
    std::vector<uint32_t> &pagePointers = clusteredIndexNode.get_page_pointers();

    SearchResult searchResult;

    if (offset >= primaryKeys.size())
    {
        return searchResult; // done already
    }

    for (size_t i = 0; i < items.size() && offset < primaryKeys.size(); i++)
    {
        const DataType &val = items[i].get(primaryColumnIndex);
        const DataType &target = *primaryKeys[offset];

        if (val > target)
        {
            if (pagePointers[i] != 0)
            {
                SearchResult result =
                    searchMultiPrimaryKeys(db_path, schema, pagePointers[i],
                                           primaryKeys, page_size, offset);
                searchResult.rows.insert(searchResult.rows.end(),
                                         std::make_move_iterator(result.rows.begin()),
                                         std::make_move_iterator(result.rows.end()));
                if (offset >= primaryKeys.size())
                {
                    return searchResult; // ✅ safe exit
                }
            }
        }

        if (offset < primaryKeys.size() && val == *primaryKeys[offset])
        {
            searchResult.rows.push_back(items[i].toRow(schema));
            offset++;
            if (offset == primaryKeys.size())
            {
                return searchResult; // ✅ stop when all found
            }
        }
    }

    // recurse into rightmost child if still have keys
    if (offset < primaryKeys.size() && pagePointers[items.size()] != 0)
    {
        SearchResult result =
            searchMultiPrimaryKeys(db_path, schema, pagePointers[items.size()],
                                   primaryKeys, page_size, offset);
        searchResult.rows.insert(searchResult.rows.end(),
                                 std::make_move_iterator(result.rows.begin()),
                                 std::make_move_iterator(result.rows.end()));
    }

    return searchResult;
}

SearchResult dbone::search::searchPrimaryKeys(const std::string &db_path, std::vector<std::unique_ptr<DataType>> &primaryKeys, uint32_t page_size)
{
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<dbone::insert::Row> rows;

    TableSchema schema(read_schema(db_path, page_size));
    size_t offset = 0;
    std::cout << "SCHEMA + RUN" << std::endl;
    SearchResult result = searchMultiPrimaryKeys(db_path, schema, *schema.clustered_page_ref, primaryKeys, page_size, offset);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    result.timeTaken = duration.count();
    return result;
}

SearchResult searchPrimaryKey(const std::string &db_path, const TableSchema &schema, uint32_t currentPage, const SearchParam &param, uint32_t page_size)
{ // from clustered index (as primary key)
    ClusteredIndexNode clusteredIndexNode = ClusteredIndexNode::load(db_path, currentPage, schema, page_size);
    std::vector<DataRow> &items = clusteredIndexNode.get_items();

    if (param.comparator == Comparator::Equal)
    {
        for (size_t i = 0; i < items.size(); i++)
        {
            if (items[i].get(*param.columnIndex) > *param.compareTo)
            {
                if (clusteredIndexNode.get_page_pointers()[i] == 0)
                {
                    return SearchResult();
                }
                return searchPrimaryKey(db_path, schema, clusteredIndexNode.get_page_pointers()[i], param, page_size);
            }
            else if (items[i].get(*param.columnIndex) == *param.compareTo)
            {
                SearchResult result;
                std::vector<dbone::insert::Row> rows;
                dbone::insert::Row row = items[i].toRow(schema);
                rows.push_back(row);
                result.rows = rows;
                return result;
            }
        }

        if (clusteredIndexNode.get_page_pointers()[items.size()] == 0)
        {
            return SearchResult();
        }
        return searchPrimaryKey(db_path, schema, clusteredIndexNode.get_page_pointers()[items.size()], param, page_size);
    }
    else if (param.comparator == Comparator::Less || param.comparator == Comparator::LessEqual)
    {
        SearchResult currentResult;
        bool end = false;
        for (size_t i = 0; i < items.size(); i++)
        {
            if (items[i].get(*param.columnIndex) < *param.compareTo)
            {
                if (clusteredIndexNode.get_page_pointers()[i] != 0)
                {
                    SearchResult result = searchPrimaryKey(db_path, schema, clusteredIndexNode.get_page_pointers()[i], param, page_size);
                    currentResult.rows.insert(currentResult.rows.end(), result.rows.begin(), result.rows.end());
                }
                dbone::insert::Row row = items[i].toRow(schema);
                currentResult.rows.push_back(row);
            }
            else if (items[i].get(*param.columnIndex) == *param.compareTo)
            {
                if (clusteredIndexNode.get_page_pointers()[i] != 0)
                {
                    SearchResult result = searchPrimaryKey(db_path, schema, clusteredIndexNode.get_page_pointers()[i], param, page_size);
                    currentResult.rows.insert(currentResult.rows.end(), result.rows.begin(), result.rows.end());
                }
                if (param.comparator == Comparator::LessEqual)
                {
                    dbone::insert::Row row = items[i].toRow(schema);
                    currentResult.rows.push_back(row);
                }
            }
            else
            {
                if (clusteredIndexNode.get_page_pointers()[i] != 0)
                {
                    SearchResult result = searchPrimaryKey(db_path, schema, clusteredIndexNode.get_page_pointers()[i], param, page_size);
                    currentResult.rows.insert(currentResult.rows.end(), result.rows.begin(), result.rows.end());
                }
                end = true;
                break;
            }
        }
        if (!end)
        {
            if (clusteredIndexNode.get_page_pointers()[items.size()] != 0)
            {
                SearchResult result = searchPrimaryKey(db_path, schema, clusteredIndexNode.get_page_pointers()[items.size()], param, page_size);
                currentResult.rows.insert(currentResult.rows.end(), result.rows.begin(), result.rows.end());
            }
        }
        return currentResult;
    }
    else if (param.comparator == Comparator::Greater || param.comparator == Comparator::GreaterEqual)
    {
        SearchResult currentResult;
        for (size_t i = 0; i < items.size(); i++)
        {
            if (items[i].get(*param.columnIndex) == *param.compareTo)
            {
                if (param.comparator == Comparator::GreaterEqual)
                {
                    dbone::insert::Row row = items[i].toRow(schema);
                    currentResult.rows.push_back(row);
                }
            }
            else if (items[i].get(*param.columnIndex) > *param.compareTo)
            {
                if (clusteredIndexNode.get_page_pointers()[i] != 0)
                {
                    SearchResult result = searchPrimaryKey(db_path, schema, clusteredIndexNode.get_page_pointers()[i], param, page_size);
                    currentResult.rows.insert(currentResult.rows.end(), result.rows.begin(), result.rows.end());
                }
                dbone::insert::Row row = items[i].toRow(schema);
                currentResult.rows.push_back(row);
            }
        }
        if (clusteredIndexNode.get_page_pointers()[items.size()] != 0)
        {
            SearchResult result = searchPrimaryKey(db_path, schema, clusteredIndexNode.get_page_pointers()[items.size()], param, page_size);
            currentResult.rows.insert(currentResult.rows.end(), result.rows.begin(), result.rows.end());
        }
        return currentResult;
    }

    return SearchResult();
}

static void searchNonIndexedAcc(
    const std::string &db_path,
    const TableSchema &schema,
    uint32_t currentPage,
    const SearchParam &param,
    uint32_t page_size,
    std::vector<dbone::insert::Row> &outRows)
{
    ClusteredIndexNode clusteredIndexNode =
        ClusteredIndexNode::load(db_path, currentPage, schema, page_size);

    std::vector<DataRow> &items = clusteredIndexNode.get_items();
    std::vector<uint32_t> &pagePointers = clusteredIndexNode.get_page_pointers();

    const DataType &compareVal = *param.compareTo; // bind once

    for (size_t i = 0; i < items.size(); ++i)
    {
        const DataType &cell = items[i].get(*param.columnIndex);

        if (param.comparator == Comparator::Less)
        {
            if (cell < compareVal)
            {
                outRows.emplace_back(items[i].toRow(schema));
            }
        }

        if (pagePointers[i] != 0)
        {
            searchNonIndexedAcc(db_path, schema, pagePointers[i], param, page_size, outRows);
        }
    }

    if (pagePointers[items.size()] != 0)
    {
        searchNonIndexedAcc(db_path, schema, pagePointers[items.size()], param, page_size, outRows);
    }
}

// Public entry point
SearchResult searchNonIndexed(
    const std::string &db_path,
    const TableSchema &schema,
    uint32_t currentPage,
    const SearchParam &param,
    uint32_t page_size)
{
    SearchResult result;
    // result.rows.reserve(40000); // pre-allocate some space to reduce growth
    searchNonIndexedAcc(db_path, schema, currentPage, param, page_size, result.rows);
    return result;
}

SearchResult dbone::search::searchItem(const std::string &db_path, const std::vector<SearchParam> &queries, uint32_t page_size)
{
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<dbone::insert::Row> rows;

    TableSchema schema(read_schema(db_path, page_size));

    std::cout << schema.columns[0].get()->name() << std::endl;

    std::unordered_map<uint16_t, std::unique_ptr<Column>> primaryColumns;

    auto &columns = schema.columns;
    for (uint16_t i = 0; i < columns.size(); i++)
    {
        std::unique_ptr<Column> &column = columns[i];
        if (column->primaryKey())
        {
            primaryColumns[i] = column->clone(); // deep copy
        }
    }

    std::cout << schema.columns[0].get()->name() << std::endl;

    if (primaryColumns.size() == 1)
    {
        // Access the only element
        auto it = primaryColumns.begin();

        uint16_t index = it->first;
        Column *column = it->second.get();
        std::unique_ptr<Column> &colPtr = it->second;

        // Example use:
        std::string columnName = column->name();
        for (const SearchParam &searchParam : queries) // keep const
        {
            if (searchParam.columnName == columnName)
            {
                SearchParam paramCopy; // construct manually
                paramCopy.columnName = searchParam.columnName;
                paramCopy.columnIndex = index;
                paramCopy.comparator = searchParam.comparator;
                paramCopy.compareTo = searchParam.compareTo->clone();
                // paramCopy.ptr left empty (or set if needed)

                SearchResult result = searchPrimaryKey(db_path, schema, *schema.clustered_page_ref, paramCopy, page_size);

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                result.timeTaken = duration.count();
                return result;
            }
            else
            {
                SearchParam paramCopy;
                paramCopy.columnName = searchParam.columnName;
                for (size_t i = 0; i < schema.columns.size(); i++)
                {
                    if (schema.columns[i].get()->name() == searchParam.columnName)
                    {
                        paramCopy.columnIndex = i;
                    }
                }
                paramCopy.comparator = searchParam.comparator;
                paramCopy.compareTo = searchParam.compareTo->clone();

                SearchResult result;

                if (schema.index_page_refs.find(*paramCopy.columnIndex) == schema.index_page_refs.end())
                {
                    result = searchNonIndexed(db_path, schema, *schema.clustered_page_ref, paramCopy, page_size);
                }
                else
                {
                    std::cout << "SEARCHING INDEXED" << std::endl;
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                result.timeTaken = duration.count();
                return result;
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    SearchResult result;
    result.timeTaken = duration.count(); // in ms
    result.rows = rows;
    return SearchResult();
}
