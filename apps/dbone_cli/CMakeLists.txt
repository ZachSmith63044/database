# apps/dbone_cli/CMakeLists.txt
add_executable(dbone_cli
  main.cpp
)

# If the CLI needs symbols from the dbone library:
target_link_libraries(dbone_cli PRIVATE dbone)

# MSVC warnings similar to your root CMake
if (MSVC)
  target_compile_options(dbone_cli PRIVATE /W4 /permissive-)
else()
  target_compile_options(dbone_cli PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
endif()
