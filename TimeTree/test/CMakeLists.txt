set(CMAKE_CXX_STANDARD 20)

set(testSources
  main.cpp
  )

include(ExternalProject)
find_package(Git REQUIRED)

ExternalProject_Add(
    doctest
    PREFIX ${CMAKE_BINARY_DIR}/doctest
    GIT_REPOSITORY https://github.com/doctest/doctest.git
    TIMEOUT 10
    UPDATE_COMMAND ${GIT_EXECUTABLE} pull
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
)

# ExternalProject_Add(
#     nanobench
#     PREFIX ${CMAKE_BINARY_DIR}/nanobench
#     GIT_REPOSITORY https://github.com/martinus/nanobench.git
#     GIT_TAG v4.1.0
#     TIMEOUT 10
#     UPDATE_COMMAND ${GIT_EXECUTABLE} pull
#     CONFIGURE_COMMAND ""
#     BUILD_COMMAND ""
#     INSTALL_COMMAND ""
#     LOG_DOWNLOAD ON
# )

include(FetchContent)

FetchContent_Declare(
    nanobench
    GIT_REPOSITORY https://github.com/martinus/nanobench.git
    GIT_TAG v4.1.0
    GIT_SHALLOW TRUE)

FetchContent_MakeAvailable(nanobench)

# Expose required variable (DOCTEST_INCLUDE_DIR) to parent scope
ExternalProject_Get_Property(doctest source_dir)
set(DOCTEST_INCLUDE_DIR ${source_dir}/doctest CACHE INTERNAL "Path to include folder for doctest")

# ExternalProject_Get_Property(nanobench source_dir)
# set(NANOBENCH_INCLUDE_DIR ${source_dir}/nanobench CACHE INTERNAL "Path to include folder for nanobench")

include_directories(${DOCTEST_INCLUDE_DIR})
# include_directories(${NANOBENCH_INCLUDE_DIR})

find_package(doctest REQUIRED)
# find_package(nanobench REQUIRED)
add_executable(tt-tests ${testSources})
# add_compile_options(-std=c++20 -O0 -g -Wall -Wextra -Wshadow -Wnon-virtual-dtor -Wunused -Wpedantic)
target_compile_options(tt-tests PUBLIC -std=c++20 -O0 -g -Wall -Wextra -Wshadow -Wnon-virtual-dtor -Wunused -Wpedantic)
target_link_libraries(tt-tests PUBLIC
  TimeTree
  # Catch2::Catch2
  doctest::doctest
  fmt
  # tl::expected  
)

add_executable(bench bench.cpp)
target_compile_options(bench PUBLIC -std=c++20 -fno-omit-frame-pointer -O3 -g -Wall -Wextra -Wshadow -Wnon-virtual-dtor -Wunused -Wpedantic)
target_link_libraries(bench PRIVATE TimeTree nanobench doctest::doctest)

# target_compile_options(tests PRIVATE -fsanitize=address)
# target_link_options(tests PRIVATE -fsanitize=address)

include(CTest)
# doctest_discover_tests(tests)
# include(Catch)
# catch_discover_tests(tests)
