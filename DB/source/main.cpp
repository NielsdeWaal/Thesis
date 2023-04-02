// #include <source/EventLoop/EventLoop.h>
#include "FileManager.hpp"
#include "InfluxParser.hpp"
#include "MemTable.hpp"
// #include "lexyparser.hpp"
#include "Query.hpp"
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
#include <memory>
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
    // co_await mInputs.ReadFromFile("../medium-plus-1");
    // co_await mInputs.ReadFromFile("../large");
    co_await mInputs.ReadFromFile("../medium-5");
    // co_await mFileManager.SetDataFiles(4);
    co_await mLogFile.OpenAt("./log.dat");
    co_await mNodeFile.OpenAt("./nodes.dat");

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
          mTrees[log->index] = std::make_unique<MetricTree>(mEv);
        }

        mTrees[log->index]->tree.Insert(log->start, log->end, log->offset);
      }
    }

    mStarted = true;
    // auto start = Common::MONOTONIC_CLOCK::Now();
    mStartTS = Common::MONOTONIC_CLOCK::Now();
  }

  // EventLoop::uio::task<> Writer(InfluxMessage& msg) {
  EventLoop::uio::task<> Writer(const IMessage& msg) {
    for (const InfluxKV& measurement : msg.measurements) {
      if (!mTrees.contains(measurement.index)) {
        mLogger->info("Creating structures for new series");
        // mTrees[measurement.index] = MetricTree{.memtable= Memtable<NULLCompressor, bufSize>{mEv}};
        mTrees[measurement.index] = std::make_unique<MetricTree>(mEv);
      }
      auto& db = mTrees[measurement.index];
      // FIXME for now we only support longs
      if (std::holds_alternative<InfluxMValue>(measurement.value)) {
        // db.memtable[db.ctr] =
        //     DataPoint{.timestamp = msg.ts, .value =
        //     std::get<std::int64_t>(std::get<InfluxMValue>(measurement.value).value)};
        // ++db.ctr;
        db->memtable.Insert(msg.ts, std::get<std::int64_t>(std::get<InfluxMValue>(measurement.value).value));
        if (db->memtable.IsFull()) {
          // EventLoop::DmaBuffer buf = mEv.AllocateDmaBuffer(bufSize);
          auto [startTS, endTS] = db->memtable.GetTimeRange();
          db->memtable.Flush(db->flushBuf);

          // EventLoop::DmaBuffer buf = db->memtable.Flush();
          // EventLoop::DmaBuffer logBuf = mEv.AllocateDmaBuffer(512);

          // std::memcpy(buf.GetPtr(), db.memtable.data(), bufSize);
          mIOQueue.push_back(WriteOperation{.buf = db->flushBuf, .pos = mFileOffset, .type = File::NODE_FILE});

          // FIXME handle case where insertion fails for whatever reason
          // db.tree.Insert(db.memtable.front().timestamp, db.memtable.back().timestamp, mFileOffset);
          db->tree.Insert(startTS, endTS, mFileOffset);

          mLogger->info(
              "Flushing memtable for {} (index: {} ts: {} - {}) to file at addr: {}",
              msg.name + "." + measurement.name,
              measurement.index,
              startTS,
              endTS,
              mFileOffset);

          LogPoint* log = ( LogPoint* ) db->logBuf.GetPtr();
          log->start = startTS;
          log->end = endTS;
          log->offset = mFileOffset;
          log->index = measurement.index;
          // co_await mLogFile.WriteAt(logBuf, logOffset);
          mIOQueue.push_back(WriteOperation{.buf = db->logBuf, .pos = mLogOffset, .type = File::LOG_FILE});

          mFileOffset += bufSize;
          mLogOffset += 512;
          db->ctr = 0;
        }
      } else {
        // Something must have gone wrong during parsing
        assert(false);
      }
      ++mIngestionCounter;
    }
    co_return;
  }

  EventLoop::uio::task<> HandleIngestion() {
    if (mStarted) {
      std::size_t chunkSize{1000};
      auto chunk = mInputs.ReadChunk(chunkSize);
      if (chunk.has_value()) {
        // mLogger->info("Reading chunk of size: {}", chunk->size());
        for (const auto& measurement : *chunk) {
          co_await Writer(measurement);
        }
      } else {
        mIngestionDone = true;
      }
    }
  }

  void OnEventLoopCallback() override {
    HandleIngestion();

    if (mIOQueue.size() > 0 && mOutstandingIO < maxOutstandingIO && mStarted) {
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

    if (mIOQueue.size() == 0 && mOutstandingIO == 0 && mStarted && mIngestionDone == true && !mQueringDone
        && !mQueryStarted) {
      mLogger->info("Testing queries");

      std::string queryTarget{"cpu.hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,"
                              "arch=x86,team=SF,service=19,service_version=1,service_environment=test,usage_user"};
      std::optional<std::uint64_t> targetIndex = mInputs.GetIndex(queryTarget);
      assert(targetIndex.has_value());
      mLogger->info("Executing query for {} (index: {})", queryTarget, targetIndex.value());

      auto nodes = mTrees[*targetIndex]->tree.Query(1451606400000000000, 1451606400000000000);
      // auto nodes = mTrees[*targetIndex]->tree.Query(1451606400000000000, 1452917110000000000);
      assert(nodes.has_value());

      std::vector<std::uint64_t> addrs;
      addrs.reserve(nodes->size());
      // std::transform(nodes->begin(), nodes->end(), addrs.begin(), [](const TimeRange_t& tr){return tr.ptr;});
      for (const TimeRange_t& tr : *nodes) {
        addrs.push_back(tr.ptr);
      }
      mLogger->info("Query requires {} reads", addrs.size());
      mRunningQueries.emplace_back(mEv, mNodeFile.GetFd(), addrs, bufSize);

      mQueryStarted = true;
    }

    if (!mRunningQueries.empty() && mQueryStarted) {
      // poll the running queries, set mQueringDone when all have been resolved
      for (Query& op : mRunningQueries) {
        if (op) {
          auto& res = op.GetResult();

          mLogger->info("Query finished for {} blocks", res.size());

          std::vector<EventLoop::DmaBuffer> resultBuffers;
          std::for_each(res.begin(), res.end(), [&resultBuffers](IOOP& resOp) {
            resultBuffers.emplace_back(std::move(resOp.buf));
          });
          for (EventLoop::DmaBuffer& buf : resultBuffers) {
            DataPoint* points = ( DataPoint* ) buf.GetPtr();

            // Expression<std::uint64_t>* tsToken = new NumberToken<std::uint64_t>(points[0].timestamp);
            // Expression<std::uint64_t>* tsConst = new NumberToken<std::uint64_t>(1452916200000000000);
            // Expression<std::uint64_t>* qValue = new NumberToken<std::uint64_t>(99);
            // for (int i = 0; i < memtableSize; ++i) {
            // Expression<std::uint64_t>* tsToken = new NumberToken<std::uint64_t>(points[i].value);
            // // bool filterRes = eq<std::uint64_t, std::uint64_t, bool>(tsToken, tsConst)();
            // bool filterRes = OrExpression<gt<std::uint64_t, std::uint64_t, bool>, gt<std::uint64_t, std::uint64_t,
            // bool>, bool>(
            //     new gt<std::uint64_t, std::uint64_t, bool>(new NumberToken<std::uint64_t>(points[i].value), new
            //     NumberToken<std::uint64_t>(99)), new gt<std::uint64_t, std::uint64_t, bool>(new
            //     NumberToken<std::uint64_t>(points[i].timestamp),
            //        new NumberToken<std::uint64_t>(1452485190000000000)))();
            // if (filterRes) {
            //   mLogger->info("res: {} -> {}", points[i].timestamp, points[i].value);
            // }
            // }
            using namespace SeriesQuery;
            // auto expr = Expr(AndExpr{LiteralExpr{2}, LiteralExpr{3}});

            // memtableSize is the size of the buffer when seen as an array of DataPoint's
            for (int i = 0; i < memtableSize; ++i) {
              // auto qRes = evaluate(Expr(AndExpr{GtExpr{LiteralExpr{points[i].value}, LiteralExpr{99}}, 
              //                                   GtExpr{LiteralExpr{points[i].timestamp}, LiteralExpr{1452600980000000000}}}));
              // auto qRes = evaluate(Expr(GtExpr{LiteralExpr{points[i].value}, LiteralExpr{99}}));
              auto qRes = evaluate(Expr(AndExpr{GtExpr{LiteralExpr{points[i].timestamp}, LiteralExpr{1452600980000000000}}, GtExpr{LiteralExpr{points[i].value}, LiteralExpr{99}}}));
              // mLogger->info("qRes: {}", qRes);
              if (qRes) {
                mLogger->info("res: {} -> {}", points[i].timestamp, points[i].value);
              }
            }
            // mLogger->info("res: {}", qRes);
          }

          std::erase(mRunningQueries, op);
          mQueringDone = true;
        }
      }
    }

    // When done, print time it took
    // TODO, instead of waiting for the queue to be empty, check with mInputs if there is anythin
    // left to be consumed
    if (mIOQueue.size() == 0 && mOutstandingIO == 0 && mStarted && mQueringDone == true) {
      auto ingestionEnd = Common::MONOTONIC_CLOCK::Now();
      auto duration = Common::MONOTONIC_CLOCK::ToNanos(ingestionEnd - mStartTS); // / 100 / 100 / 100;

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
    std::int64_t value;
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
    EventLoop::DmaBuffer& buf;
    std::uint64_t pos;
    File type;
  };

  static constexpr std::size_t maxOutstandingIO{48};
  static constexpr std::size_t bufSize{2097152}; // 2MB
  // static constexpr std::size_t bufSize{4096}; // 4KB
  static constexpr std::size_t memtableSize = bufSize / sizeof(DataPoint);

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
  bool mIngestionDone{false};
  bool mQueringDone{false};
  bool mQueryStarted{false};
  // std::vector<AppendOnlyFile> mFiles;

  Common::MONOTONIC_TIME mStartTS{};
  Common::MONOTONIC_TIME mEndTS{};

  InputManager mInputs;
  // TimeTree<64> mTree;
  // std::unordered_map<std::string, MetricTree> mTrees;
  // std::unordered_map<std::uint64_t, MetricTree> mTrees;
  std::unordered_map<std::uint64_t, std::unique_ptr<MetricTree>> mTrees;

  std::vector<Query> mRunningQueries;
  // std::unordered_map<std::string, std::uint64_t> mIndex;
  // std::uint64_t mIndexCounter{0};
};

int main() {
  // auto testing = lexy::zstring_input<lexy::utf8_encoding>("weather,location=us-midwest,foo=bar
  // temperature=82i,humidity=14.0 1465839830100400200"); assert(lexy::match<grammar::InfluxMessage>(testing) == true);
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
