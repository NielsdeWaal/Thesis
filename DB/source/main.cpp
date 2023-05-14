// #include <source/EventLoop/EventLoop.h>
#include "FileManager.hpp"
#include "FrogFishTesting.hpp"
#include "InfluxParser.hpp"
#include "IngestionPort.hpp"
#include "ManagementPort.hpp"
#include "MemTable.hpp"
// #include "lexyparser.hpp"
#include "IngestionProtocol/proto.capnp.h"
#include "MetaData.hpp"
#include "Query.hpp"
#include "TSC.h"
#include "Writer.hpp"

#include <arrow/array.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
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
#include <UDPSocket.h>
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
  // , mLogFile(mEv)
  // , mNodeFile(mEv)
  , mTSIndexFile(mEv)
  , mTestFile(mEv)
  , mFileManager(mEv) // , mSocket(mEv, this)
  // , mInputs(mEv)
  , mMetaData(mEv)
  , mWriter(mEv, mMetaData, maxOutstandingIO)
  , mIngestPort(mEv, mWriter)
  , mManagement(mEv, mMetaData)
  // , mQueryManager(mEv)
  {
    mLogger = mEv.RegisterLogger("FrogFishDB");
    mEv.RegisterCallbackHandler(( EventLoop::IEventLoopCallbackHandler* ) this, EventLoop::EventLoop::LatencyType::Low);
    // mInputs = InputManager(mEv, "../large-5");
    // mFiles.reserve(4);
    // func();
    // Writer();
    // auto _ = mTestingHelper.SetLoadingData("../large.arrow");
    // auto _ = mTestingHelper.SetLoadingData("../small-1.capfile");
    // auto _ = mTestingHelper.SetLoadingData("../large.capfile");
    IngestionTask();


    // FIXME move to test
    // MetaData data;
    // data.Insert({{"host", "01"}, {"rack", "1"}});
    // data.Insert({{"host", "02"}, {"rack", "1"}});

    // auto ids = data.QueryValues("rack", {"1"});
    // assert(ids.has_value());
    // assert(ids->size() == 2);
    // ids = data.QueryValues("host", {"01"});
    // mLogger->warn("Size: {}", ids->size());
    // assert(ids.has_value());
    // assert(ids->size() == 1);
  }

  void Configure() {
    // TODO fix these settings
    auto config = mEv.GetConfigTable("DB");
    // mIngestionMethod = config->get_as<int>("IngestionMethod").value_or(-1);
    mLogger->info(
        "Configured:\n\t testing file: {}\n\t Ingest port base: {}",
        config->get_as<std::string>("file").value_or(""),
        config->get_as<std::uint16_t>("IngestBasePort").value_or(0));
    [[maybe_unused]] auto _ = mTestingHelper.SetLoadingData(config->get_as<std::string>("file").value_or(""));
    mIngestPort.Configure(config->get_as<std::uint16_t>("IngestBasePort").value_or(0));
  }

  EventLoop::uio::task<> IngestionTask() {
    // co_await mInputs.ReadFromFile("../medium-plus-1");
    // co_await mInputs.ReadFromFile("../large");
    assert(mTestingHelper.IsLoadingData());
    co_await mMetaData.Startup();
    co_await mWriter.Configure();
    mLoadingStarted = true;
    // co_await mInputs.ReadFromFile(mTestingHelper.GetFilename());
    // mInputs.ReadFromArrowFile(mTestingHelper.GetFilename());
    // co_await mInputs.ReadFromCapnpFile(mTestingHelper.GetFilename());
    // co_await mFileManager.SetDataFiles(4);

    mStarted = true;
    // auto start = Common::MONOTONIC_CLOK::Now();
    mStartTS = Common::MONOTONIC_CLOCK::Now();
    // auto loadingLatency = mTestingHelper.SetIngesting();
    auto loadingLatency = mTestingHelper.SetPrepareQuery();
    mLogger->info("Loading data took: {}", loadingLatency);
  }

  // EventLoop::uio::task<> Writer(InfluxMessage& msg) {
  // EventLoop::uio::task<> Writer(const IMessage& msg) {
  //   // EventLoop::uio::task<> Writer(const proto::Batch::Message& msg) {
  //   // mLogger->info("Writing for {} measurements", msg.measurements.size());
  //   // mLogger->info("Ingesting {}", msg.getMetric());
  //   for (const InfluxKV& measurement : msg.measurements) {
  //     if (!mTrees.contains(measurement.index)) {
  //       mLogger->info("Creating structures for new series");
  //       // mTrees[measurement.index] = MetricTree{.memtable= Memtable<NULLCompressor, bufSize>{mEv}};
  //       mTrees[measurement.index] = std::make_unique<MetricTree>(mEv);
  //     }
  //     auto& db = mTrees[measurement.index];
  //     // FIXME for now we only support longs
  //     if (std::holds_alternative<InfluxMValue>(measurement.value)) {
  //       // db.memtable[db.ctr] =
  //       //     DataPoint{.timestamp = msg.ts, .value =
  //       //     std::get<std::int64_t>(std::get<InfluxMValue>(measurement.value).value)};
  //       // ++db.ctr;
  //       db->memtable.Insert(msg.ts, std::get<std::int64_t>(std::get<InfluxMValue>(measurement.value).value));
  //       if (db->memtable.IsFull()) {
  //         // EventLoop::DmaBuffer buf = mEv.AllocateDmaBuffer(bufSize);
  //         auto [startTS, endTS] = db->memtable.GetTimeRange();
  //         db->memtable.Flush(db->flushBuf);

  //         // EventLoop::DmaBuffer buf = db->memtable.Flush();
  //         // EventLoop::DmaBuffer logBuf = mEv.AllocateDmaBuffer(512);

  //         // std::memcpy(buf.GetPtr(), db.memtable.data(), bufSize);
  //         mIOQueue.push_back(WriteOperation{.buf = db->flushBuf, .pos = mFileOffset, .type = File::NODE_FILE});

  //         // FIXME handle case where insertion fails for whatever reason
  //         // db.tree.Insert(db.memtable.front().timestamp, db.memtable.back().timestamp, mFileOffset);
  //         db->tree.Insert(startTS, endTS, mFileOffset);

  //         mLogger->info(
  //             "Flushing memtable for {} (index: {} ts: {} - {}) to file at addr: {}",
  //             msg.name + "." + measurement.name,
  //             measurement.index,
  //             startTS,
  //             endTS,
  //             mFileOffset);

  //         LogPoint* log = ( LogPoint* ) db->logBuf.GetPtr();
  //         log->start = startTS;
  //         log->end = endTS;
  //         log->offset = mFileOffset;
  //         log->index = measurement.index;
  //         // co_await mLogFile.WriteAt(logBuf, logOffset);
  //         mIOQueue.push_back(WriteOperation{.buf = db->logBuf, .pos = mLogOffset, .type = File::LOG_FILE});

  //         mFileOffset += bufSize;
  //         mLogOffset += 512;
  //         db->ctr = 0;
  //       }
  //     } else {
  //       // Something must have gone wrong during parsing
  //       assert(false);
  //     }
  //     ++mIngestionCounter;
  //   }
  //   co_return;
  // }

  // EventLoop::uio::task<> HandleCapnpIngestion() {
  //   if (mStarted) {
  //     auto batch = mInputs.GetCapReader();
  //     std::string name;
  //     name.reserve(255);
  //     if (batch.has_value()) {
  //       if (batch.value().size() == 0) {
  //         mIngestionDone = true;
  //         // mLogger->info("Ingestion done at: {}", mTestingHelper.GetIngestionLatency());
  //         mIngestionLatency = mTestingHelper.SetPrepareQuery();
  //         mLogger->info("Ingestion took: {}", mIngestionLatency);
  //         co_return;
  //       }
  //       capnp::FlatArrayMessageReader message(batch.value());
  //       proto::Batch::Reader chunk = message.getRoot<proto::Batch>();
  //       for (const proto::Batch::Message::Reader msg : chunk.getRecordings()) {
  //         name = msg.getMetric();
  //         name.append(",");
  //         auto tags = msg.getTags();
  //         std::for_each(tags.begin(), tags.end(), [&](proto::Tag::Reader tag) {
  //           name.append(std::string{tag.getName().cStr()} + "=" + tag.getValue().cStr() + ",");
  //         });
  //         for (const proto::Batch::Message::Measurement::Reader measurement : msg.getMeasurements()) {
  //           name.append(measurement.getName());

  //           ++mIngestionCounter;

  //           mWriter.Insert(name, msg.getTimestamp(), measurement.getValue());

  //           name.resize(name.size() - measurement.getName().size());
  //         }
  //       }
  //       name.clear();
  //       mInputs.PushCapOffset(const_cast<capnp::word*>(message.getEnd()));
  //     }
  //   }
  //   // if (!mLoadingStarted && mTestingHelper.IsLoadingData()) {
  //   //   co_await IngestionTask();
  //   // }
  //   co_return;
  // }

  // EventLoop::uio::task<> HandleIngestion() {
  //   if (mStarted) {
  //     std::size_t chunkSize{1000};
  //     auto chunk = co_await mInputs.ReadArrowChunk();
  //     // auto& chunk = co_await mInputs.ReadCapChunk(chunkSize);
  //     // std::optional<std::vector<IMessage>> chunk = co_await mInputs.ReadArrowChunk();
  //     if (chunk.has_value()) {
  //       // mLogger->info("Reading chunk of size: {}", chunk->size());
  //       for (const auto& measurement : *chunk) {
  //         // for (const auto& measurement : chunk) {
  //         co_await Writer(measurement);
  //       }
  //     } else {
  //       // Finished with ingestion - switching to query tests
  //       mIngestionDone = true;
  //       // mLogger->info("Ingestion done at: {}", mTestingHelper.GetIngestionLatency());
  //       mIngestionLatency = mTestingHelper.SetPrepareQuery();
  //       mLogger->info("Ingestion took: {}", mIngestionLatency);
  //     }
  //   }
  // }

  void OnEventLoopCallback() override {
    if (mTestingHelper.IsIngesting()) {
      // HandleIngestion();
      // HandleCapnpIngestion();
    }

    if ( // mIOQueue.size() == 0 &&
        mWriter.GetOutstandingIO() == 0 && mTestingHelper.IsPreparingQuery()) {
      mLogger->info("Testing queries");

      // mQueryManager.CreateQuery("(+ 1234 123)");
      // mQueryManager.Parse("(+ 1234 (* 1 2))");
      // mQueryManager.Parse("(->> (index 50))");
      // mQueryManager.Parse("(->> (index (list 1 2)))");
      // mQueryManager.Parse("(->> (index 'usage_user'))");
      // mQueryManager.Parse("(->> (tag 'hostname'))");

      // std::string queryTarget{
      //     "cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,"
      //     "service=19,service_version=1,service_environment=test,usage_guest_nice"};
      // std::string
      // queryTarget{"cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,"
      //                         "arch=x86,team=SF,service=19,service_version=1,service_environment=test,usage_user"};
      std::string queryTarget{"hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch="
                              "x86,team=SF,service=19,service_version=1,service_environment=test,usage_guest"};

      mMetaData.ReloadIndexes();

      auto tagRes = mMetaData.QueryValues("hostname", {"host_0"});
      assert(tagRes.has_value());
      mLogger->warn("tag; {}", fmt::join(tagRes.value(), ", "));

      // std::optional<std::uint64_t> targetIndex = mInputs.GetIndex(queryTarget);
      std::optional<std::uint64_t> targetIndex = mMetaData.GetIndexByName(queryTarget);
      // std::optional<std::uint64_t> targetIndex = std::nullopt;
      // assert(targetIndex.has_value());
      if (!targetIndex.has_value()) {
        mLogger->warn("Query target {} not found, skipping query test", queryTarget);
        mQueryLatency = mTestingHelper.Finalize();
        return;
      }
      mLogger->info("Executing query for {} (index: {})", queryTarget, targetIndex.value());

      // auto nodes = mTrees[*targetIndex]->tree.Query(1464660970000000000, 1464739190000000000);
      // auto nodes = mTrees[*targetIndex]->tree.Query(1451606400000000000, 1452917110000000000);
      auto nodes = mWriter.GetTreeForIndex(targetIndex.value()).Query(1451606400000000000, 1452917110000000000);
      // auto nodes = mTrees[*targetIndex]->tree.Query(1451606400000000000, 1452917110000000000);
      assert(nodes.has_value());

      std::vector<std::uint64_t> addrs;
      addrs.reserve(nodes->size());
      // std::transform(nodes->begin(), nodes->end(), addrs.begin(), [](const TimeRange_t& tr){return tr.ptr;});
      for (const TimeRange_t& tr : *nodes) {
        addrs.push_back(tr.ptr);
      }

      mLogger->info("Query requires {} reads, starting...", addrs.size());

      // auto filter = [](SeriesQuery::UnsignedLiteralExpr ts, SeriesQuery::SignedLiteralExpr val) {
      //   using namespace SeriesQuery;
      //   return evaluate(
      //       Expr(AndExpr{GtExpr{ts, UnsignedLiteralExpr{1452606760000000000}}, GtExpr{val, SignedLiteralExpr{99}}}));
      // };
      // mTestingHelper.SetQuerying(queryTarget, filter);
      mTestingHelper.SetQuerying(queryTarget);

      // TODO support multiple starting multiple queries
      mRunningQueries.emplace_back(mEv, mWriter.GetNodeFileFd(), addrs, bufSize);
      mQueryStarted = true;
    }

    if (!mRunningQueries.empty() && mTestingHelper.IsQuerying()) {
      // if (!mRunningQueries.empty()) {
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

            // using namespace SeriesQuery;

            // // memtableSize is the size of the buffer when seen as an array of DataPoint's
            // for (std::size_t i = 0; i < memtableSize; ++i) {
            //   if (mTestingHelper.ExecFilter(points[i].timestamp, points[i].value)) {
            //     mLogger->info("res: {} -> {}", points[i].timestamp, points[i].value);
            //   }
            // }
            //
            // AndExpr{
            // LtExpr{TimestampLiteral{}, UnsignedLiteralExpr{1451621760000000000}},
            // GtExpr{ValueLiteral{}, SignedLiteralExpr{95}}}
            using namespace SeriesQuery;
            // SeriesQuery::Expr query = AndExpr{
            //     LtExpr{TimestampLiteral{}, UnsignedLiteralExpr{1451621760000000000}},
            //     GtExpr{ValueLiteral{}, SignedLiteralExpr{95}}};
            Expr query = AndExpr{};
            // add(query, LtExpr{TimestampLiteral{}, UnsignedLiteralExpr{1451621760000000000}});
            add(query, LtExpr{});
            add(query, TimestampLiteral{});
            add(query, UnsignedLiteralExpr{1451621760000000000});
            add(query, GtExpr{});
            add(query, ValueLiteral{});
            add(query, SignedLiteralExpr{95});
            // Expr query;
            // add(query, AndExpr{LtExpr{TimestampLiteral{}, UnsignedLiteralExpr{1451621760000000000}}, GtExpr{ValueLiteral{}, SignedLiteralExpr{95}}});
            for (std::size_t i = 0; i < memtableSize; ++i) {
              if (evaluate(query, points[i].timestamp, points[i].value)) {
                mLogger->info("res: {} -> {}", points[i].timestamp, points[i].value);
              }
            }
          }

          std::erase(mRunningQueries, op);
          mQueringDone = true;
          mQueryLatency = mTestingHelper.Finalize();
        }
      }
    }

    // When done, print time it took
    // TODO, instead of waiting for the queue to be empty, check with mInputs if there is anythin
    // left to be consumed
    if ( // mIOQueue.size() == 0 &&
        mWriter.GetOutstandingIO() == 0 && mStarted && mQueringDone == true && mDone == false) {
      // auto ingestionEnd = Common::MONOTONIC_CLOCK::Now();
      // auto duration = Common::MONOTONIC_CLOCK::ToNanos(ingestionEnd - mStartTS); // / 100 / 100 / 100;
      mLogger->info("Ingestion took: {}ms, query took: {}ms", mIngestionLatency / 1000000, mQueryLatency / 1000000);
      auto duration = mIngestionLatency;

      double timeTakenS = duration / 1000000000.;
      double rateS = mIngestionCounter / timeTakenS;
      double dataRate = (rateS * sizeof(DataPoint)) / 1000000;
      mLogger->info(
          "Ingestion took, {}ns, {} points, {} points/sec, {} MB/s",
          duration,
          mIngestionCounter,
          rateS,
          dataRate);
      // mEv.Stop();
      mDone = true;
    }
  }

  // void IOResolveCallback(int result) {
  //   mWriter.GetOutstandingIO() -= 1;
  // }


private:
  struct DataPoint {
    std::uint64_t timestamp;
    std::int64_t value;
  };
  struct TSIndexLog {
    std::size_t len;
    char* name;
    std::uint64_t index;
  };
  // struct WriteOperation {
  //   EventLoop::DmaBuffer& buf;
  //   std::uint64_t pos;
  //   File type;
  // };

  static constexpr std::size_t maxOutstandingIO{48};
  // static constexpr std::size_t bufSize{4194304}; // 4MB
  // static constexpr std::size_t bufSize{2097152}; // 2MB
  static constexpr std::size_t bufSize{4096}; // 4KB
  // static constexpr std::size_t bufSize{64}; // 64B
  static constexpr std::size_t memtableSize = bufSize / sizeof(DataPoint);


  struct IngestionPoint {
    std::string series;
    std::uint64_t timestamp;
    std::uint64_t value;
  };

  EventLoop::EventLoop& mEv;
  AppendOnlyFile mTSIndexFile;
  std::uint64_t mIngestionCounter{0};
  std::shared_ptr<spdlog::logger> mLogger;
  // Common::StreamSocketServer mSocket;

  DmaFile mTestFile;
  FileManager mFileManager;
  MetaData mMetaData;
  Writer<bufSize> mWriter;
  IngestionPort<bufSize> mIngestPort;
  ManagementPort mManagement;
  // QueryManager mQueryManager;
  // int mIngestionMethod{-1};

  // rigtorp::SPSCQueue<InfluxMessage> mQueue{32};
  // rigtorp::SPSCQueue<std::pair<EventLoop::SqeAwaitable, EventLoop::deferred_resolver>> mIOQueue{32};
  // rigtorp::SPSCQueue<EventLoop::deferred_resolver> mIOQueue{32};
  // std::deque<WriteOperation> mIOQueue{};
  bool mStarted{false};
  // bool mIngestionDone{false};
  bool mQueringDone{false};
  bool mQueryStarted{false};
  bool mDone{false};
  bool mLoadingStarted{false};
  // std::vector<AppendOnlyFile> mFiles;

  Common::MONOTONIC_TIME mStartTS{};
  Common::MONOTONIC_TIME mEndTS{};
  std::uint64_t mIngestionLatency{0};
  std::uint64_t mQueryLatency{0};

  // InputManager mInputs;
  // TimeTree<64> mTree;
  // std::unordered_map<std::string, MetricTree> mTrees;
  // std::unordered_map<std::uint64_t, MetricTree> mTrees;

  std::vector<Query> mRunningQueries;
  TestingHelper mTestingHelper;
  // std::unordered_map<std::string, std::uint64_t> mIndex;
  // std::uint64_t mIndexCounter{0};
};

// struct UdpServer: public Common::IUDPSocketHandler {
// public:
//   UdpServer(EventLoop::EventLoop& ev): socket(ev, this) {
//     mLogger = ev.RegisterLogger("udp server");
//   }

//   void Configure() {
//     socket.StartListening(nullptr, 8080);
//   }

//   void OnIncomingData([[maybe_unused]] char* data, [[maybe_unused]] size_t len) override {
//     mLogger->info("Received udp frame: {}", std::string{data, len});
//   }

// private:
//   Common::UDPSocket socket;
//   std::shared_ptr<spdlog::logger> mLogger;
// };

// class client: public Common::IStreamSocketHandler {
// public:
//   client(EventLoop::EventLoop& ev): mEv(ev), mSocket(ev, this) {
//     mLogger = mEv.RegisterLogger("TCPClient");
//     mSocket.Connect("127.0.0.1", 8080);
//   }

//   void OnConnected() final {
//     ::capnp::MallocMessageBuilder response;
//     proto::IdRequest::Builder request = response.initRoot<proto::IdRequest>();
//     // request.

//     // mSocket.Send();
//   }

//   // void OnDisconnected([[maybe_unused]] Common::StreamSocket* conn) final {}

//   void OnIncomingData([[maybe_unused]] Common::StreamSocket* conn, char* data, std::size_t len) final {}

// private:
//   EventLoop::EventLoop& mEv;
//   Common::StreamSocket mSocket;
//   std::shared_ptr<spdlog::logger> mLogger;
// };

int main() {
  EventLoop::EventLoop loop;
  loop.LoadConfig("FrogFish.toml");
  loop.Configure();

  Handler app(loop);
  app.Configure();

  // UdpServer app(loop);
  // app.Configure();

  loop.Run();

  return 0;
}
