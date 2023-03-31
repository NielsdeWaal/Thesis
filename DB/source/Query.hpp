#ifndef __QUERY
#define __QUERY

#include "DmaBuffer.h"
#include "EventLoop.h"
#include "UringCommands.h"
#include <cstdint>
#include <memory>
#include <vector>

struct IOOP {
  std::uint64_t pos;
  std::size_t len;
  EventLoop::DmaBuffer buf;
};

class Query: EventLoop::IUringCallbackHandler {
public:
  Query() = default;

  Query(EventLoop::EventLoop& ev, int fd, std::vector<IOOP> ops) {}
  Query(EventLoop::EventLoop& ev, int fd, std::vector<std::uint64_t> addrs, std::size_t blockSize) {
    mInFlightIO.reserve(addrs.size());
    for (const std::uint64_t addr : addrs) {
      std::unique_ptr<EventLoop::UserData> data = std::make_unique<EventLoop::UserData>();
      EventLoop::DmaBuffer buf = ev.AllocateDmaBuffer(blockSize);

      data->mCallback = this;
      data->mType = EventLoop::SourceType::Read;
      data->mInfo = EventLoop::READ{.fd = fd, .buf = buf.GetPtr(), .len = blockSize, .pos = addr};

      ev.QueueStandardRequest(std::move(data));

      mInFlightIO.push_back(IOOP{.pos = addr, .len = blockSize, .buf = std::move(buf)});
    }
  }

  operator bool() const {
    return mInFlightIO.empty();
  }

  void OnCompletion(EventLoop::CompletionQueueEvent& cqe, const EventLoop::UserData* data) override {
  }

private:
  std::vector<IOOP> mInFlightIO{};
};

#endif // __QUERY
