add_compile_options(-std=c++20 -Wall -g -O3 -Wextra -Wshadow -Wnon-virtual-dtor -Wunused -Wpedantic)

add_compile_definitions(SPDLOG_FMT_EXTERNAL)
# add_compile_definitions(LEXY_USER_CONFIG_HEADER "lexy_user_config.hpp")
set(LEXY_USER_CONFIG_HEADER "lexy_user_config.hpp")

find_package(CapnProto REQUIRED)
include_directories(${CAPNP_INCLUDE_DIRS})
add_definitions(${CAPNP_DEFINITIONS})
capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS IngestionProtocol/proto.capnp)

# add_subdirectory(third-party/hat-trie)

add_executable(FrogFishClient
  main.cpp
  FrogFishClient.hpp
  ManagementPort.hpp
  IngestionProtocol/proto.capnp.h )

# target_include_directories(FrogFish PUBLIC c)
target_link_libraries(FrogFishClient
  EventLoop
  Common

  spdlog
  fmt
  arrow
  capnp
  kj
  )
  # ${CAPNP_LIBRARIES})

# add_dependencies(FrogFish
#   CommonLibs)

target_include_directories(FrogFishClient PUBLIC ${commonlibs_SOURCE_DIR})
