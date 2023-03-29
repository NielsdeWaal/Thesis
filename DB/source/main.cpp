// #include <source/EventLoop/EventLoop.h>
#include "FileManager.hpp"
#include "InfluxParser.hpp"
// #include "lexyparser.hpp"
#include "TSC.h"

#include <charconv>
#include <EventLoop.h>
#include <File.h>
#include <fmt/core.h>
#include <iomanip>
#include <lexy/action/parse.hpp>
#include <lexy/input/file.hpp>
#include <lexy/input/string_input.hpp>
#include <lexy_ext/report_error.hpp>
#include <ranges>
// #include <rigtorp/SPSCQueue.h>
#include <src/TimeTree.hpp>
#include <sstream>
#include <string>
#include <string_view>

class Storage {
public:
  virtual ~Storage() = 0;
  virtual void InsertDataBlock(EventLoop::DmaBuffer) = 0;
  virtual EventLoop::DmaBuffer ReadDataBlock(std::uint64_t addr) = 0;
  virtual void InsertLogStatement(std::uint64_t start, std::uint64_t end, std::uint64_t pos) = 0;
};

class Handler
: public EventLoop::IEventLoopCallbackHandler
//: public Common::IStreamSocketServerHandler
//: Common::StreamSocketServer
{
public:
  explicit Handler(EventLoop::EventLoop& ev)
  : mEv(ev)
  , mLogFile(mEv)
  , mNodeFile(mEv)
  , mTSIndexFile(mEv)
  , mTestFile(mEv)
  , mFileManager(mEv) // , mSocket(mEv, this)
  , mInputs(mEv) {
    mLogger = mEv.RegisterLogger("FrogFishDB");
    mEv.RegisterCallbackHandler(( EventLoop::IEventLoopCallbackHandler* ) this, EventLoop::EventLoop::LatencyType::Low);
    // mInputs = InputManager(mEv, "../large-5");
    // mFiles.reserve(4);
    // func();
    // Writer();
    IngestionTask();
  }

  void Configure() {
    // TODO fix these settings
    auto config = mEv.GetConfigTable("DB");
    mIngestionMethod = config->get_as<int>("IngestionMethod").value_or(-1);
  }

  EventLoop::uio::task<> IngestionTask() {
    co_await mInputs.ReadFromFile("../medium-plus-1");
    // co_await mFileManager.SetDataFiles(4);
    co_await mLogFile.OpenAt("./log.dat");
    co_await mNodeFile.OpenAt("./nodes.dat");

    if(mLogFile.FileSize() > 0) {
      mLogger->info("Found existing log file, recreating indexing structures");
      std::size_t numBlocks = mLogFile.FileSize() / 512;
      mLogger->info("Reading {} blocks", numBlocks);
      for(std::size_t i = 0; i < numBlocks; ++i) {
        EventLoop::DmaBuffer logBuf = co_await mLogFile.ReadAt(i * 512, 512);
        LogPoint* log = ( LogPoint* ) logBuf.GetPtr();

        mLogger->info("Found insertion with index: {} for range: ({} -> {}) at offset: {}", log->index, log->start, log->end, log->offset);

        if(!mTrees.contains(log->index)) {
          mTrees[log->index] = MetricTree{};
        }

        mTrees[log->index].tree.Insert(log->start, log->end, log->offset);
      }
    }

    mStarted = true;
    // auto start = Common::MONOTONIC_CLOCK::Now();
    mStartTS = Common::MONOTONIC_CLOCK::Now();

  }

  // EventLoop::uio::task<> Writer(InfluxMessage& msg) {
  EventLoop::uio::task<> Writer(IMessage& msg) {
    for (const InfluxKV& measurement : msg.measurements) {
      if (!mTrees.contains(measurement.index)) {
        mLogger->info("Creating structures for new series");
        mTrees[measurement.index] = MetricTree{};
      }
      auto& db = mTrees[measurement.index];
      // FIXME for now we only support longs
      if (std::holds_alternative<InfluxMValue>(measurement.value)) {
        db.memtable[db.ctr] =
            DataPoint{.timestamp = msg.ts, .value = std::get<std::int64_t>(std::get<InfluxMValue>(measurement.value).value)};
        ++db.ctr;
        if (db.ctr == memtableSize) {
          EventLoop::DmaBuffer buf = mEv.AllocateDmaBuffer(bufSize);
          EventLoop::DmaBuffer logBuf = mEv.AllocateDmaBuffer(512);

          std::memcpy(buf.GetPtr(), db.memtable.data(), bufSize);
          mIOQueue.push_back(WriteOperation{.buf = std::move(buf), .pos = mFileOffset, .type = File::NODE_FILE});

          mLogger->info("Flushing memtable for {} (index: {}) to file at addr: {}", msg.name + "." + measurement.name, measurement.index, mFileOffset);

          // FIXME handle case where insertion fails for whatever reason
          db.tree.Insert(db.memtable.front().timestamp, db.memtable.back().timestamp, mFileOffset);

          LogPoint* log = ( LogPoint* ) logBuf.GetPtr();
          log->start = db.memtable.front().timestamp;
          log->end = db.memtable.back().timestamp;
          log->offset = mFileOffset;
          log->index = measurement.index;
          // co_await mLogFile.WriteAt(logBuf, logOffset);
          mIOQueue.push_back(WriteOperation{.buf = std::move(logBuf), .pos = mLogOffset, .type = File::LOG_FILE});

          mFileOffset += bufSize;
          mLogOffset += 512;
          db.ctr = 0;
        }
      }
      ++mIngestionCounter;
    }
    co_return;
  }

  EventLoop::uio::task<> HandleIngestion() {
    if(mStarted) {
      std::size_t chunkSize{1000};
      for (auto chunk = mInputs.ReadChunk(chunkSize); chunk.has_value(); chunk = mInputs.ReadChunk(chunkSize)) {
        mLogger->info("Reading chunk of size: {}", chunk->size());
        for (auto& measurement : *chunk) {
          // for (auto& measurement : mInputs) {
          // for (const auto& measurement : mInputs.ReadChunk)
          // TODO move writer task away from here and on to eventloop callback function
          // Only write one chunk every cycle
          co_await Writer(measurement);
        }
      }
    }
  }

  void OnEventLoopCallback() override {
    HandleIngestion();
    
    if (mOutstandingIO < maxOutstandingIO && mStarted) {
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

    // When done, print time it took
    if (mIOQueue.size() == 0 && mStarted) {
      auto end = Common::MONOTONIC_CLOCK::Now();
      auto duration = Common::MONOTONIC_CLOCK::ToNanos(end - mStartTS); // / 100 / 100 / 100;

      double timeTakenS = duration / 1000000000.;
      double rateS = mIngestionCounter / timeTakenS;
      double dataRate = (rateS * sizeof(DataPoint)) / 1000000;
      mLogger->info(
          "Ingestion took, {}ns, {} points, {} points/sec, {} MB/s",
          duration,
          mIngestionCounter,
          rateS,
          dataRate);
      mEv.Stop();
    }
  }

  void IOResolveCallback(int result) {
    mOutstandingIO -= 1;
  }


private:
  enum class File : std::uint8_t {
    NODE_FILE = 0,
    LOG_FILE,
  };
  struct DataPoint {
    std::uint64_t timestamp;
    std::uint64_t value;
  };
  struct LogPoint {
    std::uint64_t start;
    std::uint64_t end;
    std::uint64_t offset;
    std::uint64_t index;
  };
  struct TSIndexLog {
    std::size_t len;
    char* name;
    std::uint64_t index;
  };
  struct WriteOperation {
    EventLoop::DmaBuffer buf;
    std::uint64_t pos;
    File type;
  };

  static constexpr std::size_t maxOutstandingIO{48};
  static constexpr std::size_t bufSize{2097152}; // 2MB
  // static constexpr std::size_t bufSize{4096}; // 4KB
  static constexpr std::size_t memtableSize = bufSize / sizeof(DataPoint);

  struct MetricTree {
    TimeTree<64> tree;
    std::size_t ctr;
    std::array<DataPoint, memtableSize> memtable{};
  };

  struct IngestionPoint {
    std::string series;
    std::uint64_t timestamp;
    std::uint64_t value;
  };
  EventLoop::EventLoop& mEv;
  DmaFile mLogFile;
  // AppendOnlyFile mNodeFile;
  DmaFile mNodeFile;
  AppendOnlyFile mTSIndexFile;
  FileManager mFileManager;
  std::uint64_t mIngestionCounter{0};
  std::shared_ptr<spdlog::logger> mLogger;
  // Common::StreamSocketServer mSocket;

  DmaFile mTestFile;
  int mIngestionMethod{-1};

  // rigtorp::SPSCQueue<InfluxMessage> mQueue{32};
  // rigtorp::SPSCQueue<std::pair<EventLoop::SqeAwaitable, EventLoop::deferred_resolver>> mIOQueue{32};
  // rigtorp::SPSCQueue<EventLoop::deferred_resolver> mIOQueue{32};
  std::deque<WriteOperation> mIOQueue{};
  std::uint64_t mOutstandingIO{0};
  bool mStarted{false};
  std::size_t mFileOffset{0};
  std::size_t mLogOffset{0};
  // std::vector<AppendOnlyFile> mFiles;

  Common::MONOTONIC_TIME mStartTS{};
  Common::MONOTONIC_TIME mEndTS{};

  InputManager mInputs;
  // TimeTree<64> mTree;
  // std::unordered_map<std::string, MetricTree> mTrees;
  std::unordered_map<std::uint64_t, MetricTree> mTrees;
  // std::unordered_map<std::string, std::uint64_t> mIndex;
  // std::uint64_t mIndexCounter{0};
};

int main() {
  // auto testing = lexy::zstring_input<lexy::utf8_encoding>("weather,location=us-midwest,foo=bar temperature=82i,humidity=14.0 1465839830100400200"); 
  // assert(lexy::match<grammar::InfluxMessage>(testing) == true);
  // auto res = lexy::parse<grammar::InfluxMessage>(testing, lexy_ext::report_error);
  // auto file = lexy::read_file<lexy::utf8_encoding>("../small-1");
  // { … }

  // auto result = lexy::parse<grammar::InfluxMessage>(file.buffer(), lexy_ext::report_error.path("../small-5"));
  // auto result = lexy::parse<grammer::production>();
  // if (result.has_value()) {
  //   auto res = result.value();
  //   fmt::print("{}", res.size());
    // IMessage& m = res.front();
    // InfluxMValue val = std::get<InfluxMValue>(m.measurements.front().value);
    // std::int64_t intVal = std::get<std::int64_t>(val.value);
    // fmt::print("read: {}\n", intVal);
    // for (const IMessage& msg : res) {
    //   fmt::print("name: {}\n", msg.name);
    //   for (auto& [k, v] : msg.tags) {
    //     fmt::print("tags: {} - {}\n", k, v);
    //   }
    //   for (auto& [k, v] : msg.measurements) {
    //     fmt::print("measurements: {} - {}\n", k, v);
    //   }
    //   fmt::print("ts: {}\n", msg.ts);
    // }
  // }

  EventLoop::EventLoop loop;
  loop.LoadConfig("FrogFish.toml");
  loop.Configure();

  Handler app(loop);
  app.Configure();

  loop.Run();

  return 0;
}
