add_compile_options(-std=c++20 -Wall -g -O0 -Wextra -Wshadow -Wnon-virtual-dtor -Wunused -Wpedantic)

add_compile_definitions(SPDLOG_FMT_EXTERNAL)
# add_compile_definitions(LEXY_USER_CONFIG_HEADER "lexy_user_config.hpp")
set(LEXY_USER_CONFIG_HEADER "lexy_user_config.hpp")

find_package(CapnProto REQUIRED)
include_directories(${CAPNP_INCLUDE_DIRS})
add_definitions(${CAPNP_DEFINITIONS})
capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS IngestionProtocol/proto.capnp)

# add_subdirectory(third-party/hat-trie)

add_executable(FrogFish
  main.cpp
  FrogFish.hpp
  FileManager.hpp
  InfluxParser.hpp
  lexyparser.hpp
  lexy_user_config.hpp
  SeriesIndex.hpp
  ManagementPort.hpp
  Writer.hpp
  MetaData.hpp
  Query.hpp
  QueryManager.hpp
  QueryPostProcessingOps.hpp
  # Query.cpp

  robin_hood.h
  IngestionProtocol/proto.capnp.h
  FrogFishTesting.hpp)

# target_include_directories(FrogFish PUBLIC c)
target_link_libraries(FrogFish 
  PRIVATE
  EventLoop
  Common
  TimeTree

  spdlog
  fmt
  foonathan::lexy
  arrow
  capnp
  kj
  tsl::hat_trie
  )
  # ${CAPNP_LIBRARIES})

# add_dependencies(FrogFish
#   CommonLibs)

target_include_directories(FrogFish PUBLIC ${commonlibs_SOURCE_DIR})
