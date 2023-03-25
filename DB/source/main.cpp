// #include <source/EventLoop/EventLoop.h>
#include "FileManager.hpp"
#include "InfluxParser.hpp"
#include "TSC.h"

#include <charconv>
#include <EventLoop.h>
#include <File.h>
#include <fmt/core.h>
#include <iomanip>
#include <ranges>
#include <rigtorp/SPSCQueue.h>
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
  , mTestFile(mEv)
  , mFileManager(mEv) // , mSocket(mEv, this)
  {
    mLogger = mEv.RegisterLogger("FrogFishDB");
    mEv.RegisterCallbackHandler(this, EventLoop::EventLoop::LatencyType::Low);
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
    // co_await mFileManager.SetDataFiles(4);
    co_await mLogFile.OpenAt("./log.dat");
    co_await mNodeFile.OpenAt("./nodes.dat");

    // std::ifstream input("../medium-plus-1");
    std::ifstream input("../large-5");
    InfluxParser parser;

    // InfluxMessage measurement;
    std::vector<InfluxMessage> messages;
    messages.reserve(200000);
    // auto duration = 0;
    for (std::string line; std::getline(input, line);) {
      messages.emplace_back();
      parser.Parse(line, messages.back());
      for (InfluxMeasurement& measurement : messages.back().measurments) {
        std::string name{messages.back().name + "." + measurement.name};
        if (!mIndex.contains(name)) {
          mIndex[name] = mIndexCounter;
          mLogger->info("New series for {}", name);
          mTrees[mIndexCounter] = MetricTree{};
          ++mIndexCounter;
        }
      }
      // mLogger->info("Wire: {} -> {} - {}", measurement.timestamp,
      //               fmt::join(measurement.tags, ", "),
      //               fmt::join(measurement.values, ", "));
      // mLogger->info("Wire: {} -> {}", measurement.timestamp,
      // fmt::join(measurement.measurments, ", "));
    }

    mStarted = true;
    // auto start = Common::MONOTONIC_CLOCK::Now();
    mStartTS = Common::MONOTONIC_CLOCK::Now();

    for (auto& measurement : messages) {
      co_await Writer(measurement);
    }


    // mEv.Stop();
    // co_return;
  }

  EventLoop::uio::task<> Writer(InfluxMessage& msg) {
    for (const InfluxMeasurement& measurement : msg.measurments) {
      auto& db = mTrees[msg.index];
      // FIXME for now we only support longs
      if (std::holds_alternative<std::uint64_t>(measurement.value)) {
        db.memtable[db.ctr] =
            DataPoint{.timestamp = msg.timestamp, .value = std::get<std::uint64_t>(measurement.value)};
        ++db.ctr;
        if (db.ctr == memtableSize) {
          // mLogger->info("Flushing memtable for {}", name);
          EventLoop::DmaBuffer buf = mEv.AllocateDmaBuffer(bufSize);
          EventLoop::DmaBuffer logBuf = mEv.AllocateDmaBuffer(512);

          std::memcpy(buf.GetPtr(), db.memtable.data(), bufSize);
          mIOQueue.push_back(WriteOperation{.buf = std::move(buf), .pos = mFileOffset, .type = File::NODE_FILE});

          db.tree.Insert(db.memtable.front().timestamp, db.memtable.back().timestamp, mFileOffset);

          LogPoint* log = ( LogPoint* ) logBuf.GetPtr();
          log->start = db.memtable.front().timestamp;
          log->end = db.memtable.back().timestamp;
          log->offset = mFileOffset;
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

  void OnEventLoopCallback() override {
    if (mOutstandingIO < 32 && mStarted) {
      if (mIOQueue.front().type == File::NODE_FILE) {
        mLogger->info("Room to issue request, writing to nodefile at pos: {}", mIOQueue.front().pos);
        EventLoop::SqeAwaitable awaitable = mNodeFile.WriteAt(mIOQueue.front().buf, mIOQueue.front().pos);
        awaitable.SetCallback([&](int res) { IOResolveCallback(res); });
      } else if (mIOQueue.front().type == File::LOG_FILE) {
        mLogger->info("Room to issue request, writing to log at pos: {}", mIOQueue.front().pos);
        // mLogFile.WriteAt(mIOQueue.front().buf, mIOQueue.front().pos);
        EventLoop::SqeAwaitable awaitable = mLogFile.WriteAt(mIOQueue.front().buf, mIOQueue.front().pos);
        awaitable.SetCallback([&](int res) { IOResolveCallback(res); });
      }
      mIOQueue.pop_front();
      ++mOutstandingIO;
      // issue operation
    }
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
  };
  struct WriteOperation {
    EventLoop::DmaBuffer buf;
    std::uint64_t pos;
    File type;
  };

  static constexpr std::size_t bufSize{4096};
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
  FileManager mFileManager;
  std::uint64_t mIngestionCounter{0};
  std::shared_ptr<spdlog::logger> mLogger;
  // Common::StreamSocketServer mSocket;

  DmaFile mTestFile;
  int mIngestionMethod{-1};

  rigtorp::SPSCQueue<InfluxMessage> mQueue{32};
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

  // TimeTree<64> mTree;
  // std::unordered_map<std::string, MetricTree> mTrees;
  std::unordered_map<std::uint64_t, MetricTree> mTrees;
  std::unordered_map<std::string, std::uint64_t> mIndex;
  std::uint64_t mIndexCounter{0};
};

int main() {
  EventLoop::EventLoop loop;
  loop.LoadConfig("FrogFish.toml");
  loop.Configure();

  Handler app(loop);
  app.Configure();

  loop.Run();

  return 0;
}
