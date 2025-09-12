cmake_minimum_required(VERSION 3.16)
project(dbone LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (MSVC)
  add_compile_options(/W4 /permissive-)
else()
  add_compile_options(-Wall -Wextra -Wpedantic -Wconversion)
endif()

# Core library
add_library(dbone
  src/schema.cpp
  src/constants.cpp
  src/storage.cpp
  src/index.cpp
  src/insert.cpp
  src/serialize.cpp
  src/columns/column.cpp
  src/columns/dataTypes.cpp
  src/ser.cpp            # ← ADD THIS so put_u32/put_bytes, etc. get linked
)
target_include_directories(dbone PUBLIC include)

# CLI app
add_subdirectory(apps/dbone_cli)
