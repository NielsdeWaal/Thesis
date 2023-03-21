cmake_minimum_required (VERSION 3.6)
project(FrogFishDB)

# External resources/repositories are downloaded here
add_subdirectory(external)

# Testing
enable_testing()
# add_subdirectory(test)

# Source code
add_subdirectory(source)