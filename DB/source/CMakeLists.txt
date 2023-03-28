add_compile_options(-std=c++20 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -Wunused -Wpedantic)

add_compile_definitions(SPDLOG_FMT_EXTERNAL)
# add_compile_definitions(LEXY_USER_CONFIG_HEADER "lexy_user_config.hpp")
set(LEXY_USER_CONFIG_HEADER "lexy_user_config.hpp")

add_executable(FrogFish
  main.cpp
  FileManager.hpp
  InfluxParser.hpp
  lexyparser.hpp
  lexy_user_config.hpp
  SeriesIndex.hpp)

# target_include_directories(FrogFish PUBLIC c)
target_link_libraries(FrogFish 
  EventLoop
  Common
  TimeTree
  spdlog
  fmt
  foonathan::lexy)

# add_dependencies(FrogFish
#   CommonLibs)

target_include_directories(FrogFish PUBLIC ${commonlibs_SOURCE_DIR})
