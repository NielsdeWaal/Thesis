#ifndef __SERIES_INDEX
#define __SERIES_INDEX

#include "File.h"
#include "Query.hpp"

#include <tsl/htrie_map.h>

#include <atomic>
#include <string>
#include <memory>
#include <unordered_map>

class Index: public Common::NonCopyable<Index> , public EventLoop::IUringCallbackHandler {
public:
  Index(EventLoop::EventLoop& ev): mStringFile(ev), mSeriesLog(ev), mEv(ev) {
    mLogger = ev.RegisterLogger("SeriesIndex");
  }

  EventLoop::uio::task<std::uint64_t> AddSeries(const std::string& seriesName) {
    assert(seriesName.size() <= 512);
    EventLoop::DmaBuffer buf = mEv.AllocateDmaBuffer(512);

    std::uint64_t id = mIndexCounter.fetch_add(1, std::memory_order_relaxed);
    SeriesLog* log = ( SeriesLog* ) buf.GetPtr();
    log->seriesIndex = id;
    log->pos = mStringFileOffset;
    log->len = seriesName.size();

    mLogger->info("New series for {}, assigning id: {}", seriesName, id);

    co_await mSeriesLog.WriteAt(buf, mLogFileOffset);

    std::memcpy(buf.GetPtr(), seriesName.data(), seriesName.length());

    co_await mStringFile.WriteAt(buf, mStringFileOffset);

    mIndex[seriesName] = id;

    mStringFileOffset += 512;
    mLogFileOffset += 512;

    co_return id;

    // co_await mLogFile.WriteAt(logBuf, logOffset);
    // mIOQueue.push_back(WriteOperation{.buf = std::move(logBuf), .pos = mLogOffset, .type = File::LOG_FILE});
  }

  [[nodiscard]] std::optional<std::uint64_t> GetIndex(const std::string& seriesName) {
    // if (!mIndex.contains(seriesName)) {
    if (mIndex.find(seriesName) == mIndex.end()) {
      return std::nullopt;
    }
    return mIndex[seriesName];
  }

  std::uint64_t InsertSeries(const std::string& seriesName) {
    assert(seriesName.size() <= 512);
    EventLoop::DmaBuffer buf = mEv.AllocateDmaBuffer(512);
    EventLoop::DmaBuffer seriesBuf = mEv.AllocateDmaBuffer(512);

    std::uint64_t id = mIndexCounter.fetch_add(1, std::memory_order_relaxed);
    SeriesLog* log = ( SeriesLog* ) buf.GetPtr();
    log->seriesIndex = id;
    log->pos = mStringFileOffset;
    log->len = seriesName.size();

    mLogger->info("New series for {}, assigning id: {}", seriesName, id);

    // co_await mSeriesLog.WriteAt(buf, mLogFileOffset);

    // mEv.SubmitRead(mSeriesLog.GetFd(), mLogFileOffset, buf.GetPtr(), 512);
    {
     int fd = mSeriesLog.GetFd();
      std::unique_ptr<EventLoop::UserData> data = std::make_unique<EventLoop::UserData>();

      data->mCallback = this;
      data->mType = EventLoop::SourceType::Write;
      data->mInfo = EventLoop::WRITE{.fd = fd, .buf = buf.GetPtr(), .len = 512, .pos = mLogFileOffset};

      mEv.QueueStandardRequest(std::move(data));

      mInFlightIO.push_back(IOOP{.pos = mLogFileOffset, .len = 512, .buf = std::move(buf)});
    }

    std::memcpy(seriesBuf.GetPtr(), seriesName.data(), seriesName.length());

    // mEv.SubmitRead(mStringFile.GetFd(), mStringFileOffset, seriesBuf.GetPtr(), 512);
    // co_await mStringFile.WriteAt(buf, mStringFileOffset);
    {
     int fd = mStringFile.GetFd();
      std::unique_ptr<EventLoop::UserData> data = std::make_unique<EventLoop::UserData>();

      data->mCallback = this;
      data->mType = EventLoop::SourceType::Write;
      data->mInfo = EventLoop::WRITE{.fd = fd, .buf = seriesBuf.GetPtr(), .len = 512, .pos = mStringFileOffset};

      mEv.QueueStandardRequest(std::move(data));

      mInFlightIO.push_back(IOOP{.pos = mStringFileOffset, .len = 512, .buf = std::move(seriesBuf)});
    }

    mIndex[seriesName] = id;

    mStringFileOffset += 512;
    mLogFileOffset += 512;

    return id;
  }
  
  void OnCompletion(EventLoop::CompletionQueueEvent& cqe, const EventLoop::UserData* data) override {
  }


  // [[nodiscard]] std::uint64_t

  // [[nodiscard]] std::string& GetSeriesName(std::uint64 index) {
  // return mIndex
  // }

  EventLoop::uio::task<> SetupFiles() {
    co_await mStringFile.OpenAt("./SeriesIndex.log");
    co_await mSeriesLog.OpenAt("./Index.log");

    if (mSeriesLog.FileSize() == 0) {
      mLogger->info("New series log");
    } else {
      mLogger->info("Found existing series log, recreating table");
      mStringFileOffset = mStringFile.FileSize();

      // EventLoop::DmaBuffer logBuf = mEv.AllocateDmaBuffer(512);
      std::size_t numBlocks = mSeriesLog.FileSize() / 512;
      mLogger->info("Reading {} blocks", numBlocks);
      for (std::size_t i = 0; i < numBlocks; ++i) {
        EventLoop::DmaBuffer logBuf = co_await mSeriesLog.ReadAt(i * 512, 512);
        SeriesLog* log = ( SeriesLog* ) logBuf.GetPtr();

        mLogger->info("Found series with index: {} at offset: {}", log->seriesIndex, log->pos);

        EventLoop::DmaBuffer stringBuf = co_await mStringFile.ReadAt(log->pos, 512);
        std::string seriesName{static_cast<char*>(stringBuf.GetPtr()), log->len};
        mIndex[seriesName] = log->seriesIndex;

        if (log->seriesIndex > mIndexCounter) {
          mIndexCounter = log->seriesIndex;
        }
      }
    }
  }

private:
  struct SeriesLog {
    std::uint64_t pos;         // position in string file
    std::size_t len;           // length of string in string file
    std::uint64_t seriesIndex; // index this string belongs to
  };

  DmaFile mStringFile;
  DmaFile mSeriesLog;

  // std::unordered_map<std::size_t, std::string> mNameMapping;
  std::atomic<std::uint64_t> mIndexCounter{1};
  std::uint64_t mStringFileOffset{0};
  std::uint64_t mLogFileOffset{0};

  // std::unordered_map<std::string, std::uint64_t> mIndex{};
  // tsl::htrie_map<std::string, std::uint64_t> mIndex{};
  tsl::htrie_map<char, std::uint64_t> mIndex{};

  std::vector<IOOP> mInFlightIO{};

  EventLoop::EventLoop& mEv;
  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __SERIES_INDEX
