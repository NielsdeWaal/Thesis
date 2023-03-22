set(CXX_STANDARD c++20)
add_compile_options(-std=c++20 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -Wunused -Wpedantic)

add_compile_definitions(SPDLOG_FMT_EXTERNAL)

add_executable(FrogFish
  main.cpp
  FileManager.hpp)

# target_include_directories(FrogFish PUBLIC c)
target_link_libraries(FrogFish 
  EventLoop
  Common
  TimeTree
  spdlog
  fmt)

# add_dependencies(FrogFish
#   CommonLibs)

target_include_directories(FrogFish PUBLIC ${commonlibs_SOURCE_DIR})
