add_compile_options(-std=c++20 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -Wunused -Wpedantic)
set(TimeTreeHeaders EventLoop.h File.h UringCommands.h Topology.h)
set(TimeTreeSources EventLoop.cpp)

# add_library(TimeTree STATIC ${EventLoopHeaders} ${EventLoopSources})
# # add_library(EventLoop::EventLoopt ALIAS EventLoop)
# target_link_libraries(TimeTree PRIVATE
#   fmt)

# target_include_directories(EventLoop
#   INTERFACE ./)
