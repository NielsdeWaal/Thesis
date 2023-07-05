#ifndef __MEMTABLE
#define __MEMTABLE

#include "DmaBuffer.h"
#include "EventLoop.h"

#include <cstddef>

class IMemtableCompressor {
public:
  virtual ~IMemtableCompressor() = default;
  virtual std::size_t GetMargin() = 0;
  virtual char* Insert(char* ptr, std::uint64_t timestamp, std::int64_t value) = 0;
};

// No compression, values are simply copied to buffer
class NULLCompressor: public IMemtableCompressor {
public:
  NULLCompressor() = default;

  char* Insert(char* ptr, std::uint64_t timestamp, std::int64_t value) override {
    std::memcpy(ptr, &timestamp, sizeof(std::uint64_t));
    ptr += sizeof(std::uint64_t);
    std::memcpy(ptr, &value, sizeof(std::int64_t));
    ptr += sizeof(std::int64_t);

    return ptr;
  }

  std::size_t GetMargin() override {
    // Need space for timestap and value
    return sizeof(std::uint64_t) + sizeof(std::int64_t);
  }
};

template<typename Compressor, std::size_t BufSize> class Memtable {
public:
  Memtable(EventLoop::EventLoop& ev)
  : mEv(ev)
  , mBuffer(ev.AllocateDmaBuffer(BufSize))
  , mFlushBuffer(ev.AllocateDmaBuffer(BufSize))
  , mPtr((char*)mBuffer.GetPtr())
  , mEnd(mPtr + BufSize) {}

  bool IsFull() {
    const std::size_t margin = mCompressor.GetMargin();
    return static_cast<std::size_t>(mEnd - mPtr) < margin;
  }

  // Flush memtable to flushable buffer
  // EventLoop::DmaBuffer Flush() {
  void Flush(EventLoop::DmaBuffer& buf) {
    // Swap buffer and flush buffer
    // std::swap(mBuffer, mFlushBuffer);
    // std::exchange();
    // mPtr = static_cast<char*>(mBuffer.GetPtr());
    // mEnd = mPtr + BufSize;
    std::memcpy(buf.GetPtr(), mBuffer.GetPtr(), BufSize);
    // std::memcpy(mFlushBuffer.GetPtr(), mBuffer.GetPtr(), BufSize);
    // EventLoop::DmaBuffer flushBuffer = mEv.AllocateDmaBuffer(BufSize);
    // std::memcpy(flushBuffer.GetPtr(), mBuffer.GetPtr(), BufSize);
    mStartTS = 0;
    mPtr = static_cast<char*>(mBuffer.GetPtr());
    // return flushBuffer;
  }

  void Insert(const std::uint64_t timestamp, const std::int64_t value) {
    if(mStartTS == 0) {
      mStartTS = timestamp;
    }
    mEndTS = timestamp;
    mPtr = mCompressor.Insert(mPtr, timestamp, value);
  }

  std::uint64_t GetTableEnd() const {
    return mEndTS;
  }

  std::pair<std::uint64_t, std::uint64_t> GetTimeRange() const {
    return std::make_pair(mStartTS, mEndTS);
  }

  void* GetData() const {
    return mBuffer.GetPtr();
  }

private:
  EventLoop::EventLoop& mEv;
  EventLoop::DmaBuffer mBuffer;
  EventLoop::DmaBuffer mFlushBuffer;

  Compressor mCompressor;

  std::uint64_t mStartTS{0};
  std::uint64_t mEndTS{0};

  // std::uint8_t ctr{0};
  char* mPtr;
  char* mEnd;
};

#endif // __MEMTABLE
