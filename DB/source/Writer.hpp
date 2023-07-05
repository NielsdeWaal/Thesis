#ifndef __WRITER_HPP
#define __WRITER_HPP

#include "EventLoop.h"
#include "File.h"
#include "InfluxParser.hpp"
#include "MemTable.hpp"
#include "MetaData.hpp"
#include "src/TimeTree.hpp"

#include <cstdint>

struct LogPoint {
  std::uint64_t start;
  std::uint64_t end;
  std::uint64_t offset;
  std::uint64_t index;
};

template<std::size_t bufSize> class Writer: public EventLoop::IEventLoopCallbackHandler {
public:
  Writer(EventLoop::EventLoop& ev, MetaData& metadata, std::size_t maxOutstanding)
  : mEv(ev)
  , mLogFile(mEv)
  , mNodeFile(mEv)
  , mMaxOutstandingIO(maxOutstanding)
  , mMetadata(metadata) {
    mEv.RegisterCallbackHandler(( EventLoop::IEventLoopCallbackHandler* ) this, EventLoop::EventLoop::LatencyType::Low);
    mLogger = mEv.RegisterLogger("Writer");
  }

  // FIXME Remove coroutine dependency
  EventLoop::uio::task<> Configure() {
    co_await mLogFile.OpenAt("./log.dat");
    co_await mNodeFile.OpenAt("./nodes.dat");

    mLogger->info("Opened files, logs: {}, nodes: {}", mLogFile.GetFd(), mNodeFile.GetFd());

    if (mLogFile.FileSize() > 0) {
      mLogger->info("Found existing log file, recreating indexing structures");
      std::size_t numBlocks = mLogFile.FileSize() / 512;
      mLogger->info("Reading {} blocks", numBlocks);
      for (std::size_t i = 0; i < numBlocks; ++i) {
        EventLoop::DmaBuffer logBuf = co_await mLogFile.ReadAt(i * 512, 512);
        LogPoint* log = ( LogPoint* ) logBuf.GetPtr();

        mLogger->info(
            "Found insertion with index: {} for range: ({} -> {}) at offset: {}",
            log->index,
            log->start,
            log->end,
            log->offset);

        if (!mTrees.contains(log->index)) {
          // mTrees[log->index] = MetricTree{.memtable = Memtable<NULLCompressor, bufSize>{mEv}};
          mLogger->info("Recreating indexing structure for index: {}", log->index);
          mTrees[log->index] = std::make_unique<MetricTree>(mEv);
        }

        mTrees[log->index]->tree.Insert(log->start, log->end, log->offset);
        mFileOffset = log->offset + bufSize;
      }
      mLogOffset = (numBlocks + 1) * 512;
    }
  }

  // void Insert(const std::string& name, std::uint64_t ts, std::int64_t value) {
  // int index{0};
  // if (auto nameIndex = mInputs.GetIndex(name); nameIndex.has_value()) {
  //   index = nameIndex.value();
  // } else {
  //   index = mInputs.InsertSeries(name);
  // }
  //   int index = mMetadata.Insert(const int &tags)
  //   WriteToIndex(index, ts, value);
  // }

  void Insert(std::uint64_t index, std::uint64_t ts, std::int64_t value) {
    WriteToIndex(index, ts, value);
  }

  std::size_t GetOutstandingIO() const {
    return mOutstandingIO;
  }

  void OnEventLoopCallback() final {
    if (mIOQueue.size() > 0 && mOutstandingIO < mMaxOutstandingIO) {
      // TODO instead of awaitables, these can just be regular uring requests
      if (mIOQueue.front().type == File::NODE_FILE) {
        mLogger->debug("Room to issue request, writing to nodefile at pos: {}", mIOQueue.front().pos);
        EventLoop::SqeAwaitable awaitable = mNodeFile.WriteAt(mIOQueue.front().buf, mIOQueue.front().pos);
        awaitable.SetCallback([&](int res) { IOResolveCallback(res); });
      } else if (mIOQueue.front().type == File::LOG_FILE) {
        mLogger->debug("Room to issue request, writing to log at pos: {}", mIOQueue.front().pos);
        // mLogFile.WriteAt(mIOQueue.front().buf, mIOQueue.front().pos);
        EventLoop::SqeAwaitable awaitable = mLogFile.WriteAt(mIOQueue.front().buf, mIOQueue.front().pos);
        awaitable.SetCallback([&](int res) { IOResolveCallback(res); });
      }
      mIOQueue.pop_front();
      ++mOutstandingIO;
      // issue operation
    }
  }

  TimeTree<64>& GetTreeForIndex(const std::uint64_t index) {
    return mTrees[index]->tree;
  }

  int GetNodeFileFd() const {
    return mNodeFile.GetFd();
  }

  // FIXME needs to do more than just decrease the counter
  void IOResolveCallback([[maybe_unused]] int result) {
    mOutstandingIO -= 1;
  }

private:
  void WriteToIndex(std::uint64_t index, std::uint64_t timestamp, std::int64_t value) {
    if (!mTrees.contains(index)) {
      mLogger->info("Creating structures for new series");
      mTrees[index] = std::make_unique<MetricTree>(mEv);
      // mLogger->warn("End: {}", mTrees[index]->tree.GetRoot()->GetNodeEnd());
    }

    auto& db = mTrees[index];
    if (timestamp < db->memtable.GetTableEnd() || timestamp < db->tree.GetRoot()->GetNodeEnd()) {
      mLogger->info(
          "Already ingested ts: {}, memtable end: {}, ptr: {}, < MemTableEnd: {}, < TreeEnd {}",
          timestamp,
          db->memtable.GetTableEnd(),
          fmt::ptr(db.get()),
          timestamp < db->memtable.GetTableEnd(),
          timestamp < db->tree.GetRoot()->GetNodeEnd());
      // name.resize(name.size() - measurement.getName().size());
      return;
    }
    db->memtable.Insert(timestamp, value);
    // mLogger->info("Ingesting: {}, timestamp: {}, value: {}", name, msg.getTimestamp() , measurement.getValue());
    if (db->memtable.IsFull()) {
      auto [startTS, endTS] = db->memtable.GetTimeRange();
      db->memtable.Flush(db->flushBuf);
      mIOQueue.push_back(WriteOperation{.buf = db->flushBuf, .pos = mFileOffset, .type = File::NODE_FILE});

      db->tree.Insert(startTS, endTS, mFileOffset);

      mLogger->info(
          "Flushing memtable for (index: {} ts: {} - {}) to file at addr: {}",
          // name,
          index,
          startTS,
          endTS,
          mFileOffset);

      LogPoint* log = ( LogPoint* ) db->logBuf.GetPtr();
      log->start = startTS;
      log->end = endTS;
      log->offset = mFileOffset;
      log->index = index;
      // co_await mLogFile.WriteAt(logBuf, logOffset);
      mIOQueue.push_back(WriteOperation{.buf = db->logBuf, .pos = mLogOffset, .type = File::LOG_FILE});

      mFileOffset += bufSize;
      mLogOffset += 512;
      db->ctr = 0;
    }
  }
  enum class File : std::uint8_t {
    NODE_FILE = 0,
    LOG_FILE,
  };
  struct MetricTree {
    MetricTree(EventLoop::EventLoop& ev)
    : memtable(ev)
    , flushBuf(ev.AllocateDmaBuffer(bufSize))
    , logBuf(ev.AllocateDmaBuffer(512)) {}
    TimeTree<64> tree;
    std::size_t ctr;
    // std::array<DataPoint, memtableSize> memtable{};
    Memtable<NULLCompressor, bufSize> memtable;
    EventLoop::DmaBuffer flushBuf;
    EventLoop::DmaBuffer logBuf;
  };
  struct WriteOperation {
    EventLoop::DmaBuffer& buf;
    std::uint64_t pos;
    File type;
  };
  EventLoop::EventLoop& mEv;
  std::shared_ptr<spdlog::logger> mLogger;

  DmaFile mLogFile;
  DmaFile mNodeFile;

  std::unordered_map<std::uint64_t, std::unique_ptr<MetricTree>> mTrees;
  std::size_t mFileOffset{0};
  std::size_t mLogOffset{0};
  std::uint64_t mOutstandingIO{0};
  std::size_t mMaxOutstandingIO{0};

  MetaData& mMetadata;
  std::deque<WriteOperation> mIOQueue{};
};

#endif // __WRITER_HPP
