#ifndef __MEMTABLE
#define __MEMTABLE

#include "DmaBuffer.h"
#include "EventLoop.h"
#include <cstddef>

template<std::size_t BufSize>
class Memtable {
public:
  Memtable(EventLoop::EventLoop& ev)
  : mBuffer(ev.AllocateDmaBuffer(BufSize)) {}

  bool IsFull() {
    return true;
  }

  void Flush() {
    
  }

  void Insert() {
    
  }

private:
  EventLoop::DmaBuffer mBuffer;
  std::size_t ctr{0};
};

#endif // __MEMTABLE
