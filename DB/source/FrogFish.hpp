#ifndef __FROGFISH_DB
#define __FROGFISH_DB

#include "EventLoop.h"
#include "File.h"
#include "FileManager.hpp"
#include "FrogFishTesting.hpp"
#include "IngestionPort.hpp"
#include "ManagementPort.hpp"
#include "MetaData.hpp"
#include "QueryManager.hpp"
#include "Writer.hpp"

class Database: public EventLoop::IEventLoopCallbackHandler {
public:
  explicit Database(EventLoop::EventLoop& ev)
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
  , mQueryManager(mEv, mWriter, mMetaData) {
    mLogger = mEv.RegisterLogger("FrogFishDB");
    mEv.RegisterCallbackHandler(( EventLoop::IEventLoopCallbackHandler* ) this, EventLoop::EventLoop::LatencyType::Low);
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

    mStarted = true;
    // auto start = Common::MONOTONIC_CLOK::Now();
    mStartTS = Common::MONOTONIC_CLOCK::Now();
    // auto loadingLatency = mTestingHelper.SetIngesting();
    auto loadingLatency = mTestingHelper.SetPrepareQuery();
    mLogger->info("Loading data took: {}", loadingLatency);
  }

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
      // std::string
      // queryTarget{"hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch="
      //                         "x86,team=SF,service=19,service_version=1,service_environment=test,usage_guest"};

      mMetaData.ReloadIndexes();

      // mQueryManager.SubmitQuery("(->>"
      //                           "(index 42)"
      //                           "(range 1451606400000000000 1464713590000000000)"
      //                           "(where (and (< #TS 1451621760000000000) (> #V 95))))");

      // mQueryManager.SubmitQuery("(->>"
      //                           "(index 50)"
      //                           "(range 1451606400000000000 1464713590000000000)"
      //                           "(where (and (< #TS 1451621760000000000) (> #V 95))))");

      // mQueryManager.SubmitQuery("(->>"
      //                           "(metric \"usage_user\")"
      //                           "(tag \"hostname\" '(\"host_0\"))"
      //                           "(range 1451606400000000000 1464713590000000000)"
      //                           "(where (and (< #TS 1451621760000000000) (> #V 95))))");

      // mQueryManager.SubmitQuery("(->>"
      //                           "(metric \"usage_user\")"
      //                           "(tag \"hostname\" '(\"host_0\"))"
      //                           "(range 1464710340000000000 1464720460000000000)"
      //                           "(groupby 1h max))");

      // mQueryManager.SubmitQuery("(->>"
      //                           "(metric \"usage_user\")"
      //                           "(tag \"hostname\" '(\"host_0\"))"
      //                           "(range 1464710340000000000 1464720460000000000)"
      //                           "(groupby 1h avg))");

      mQueryManager.SubmitQuery("(->>"
                                "(metric \"usage_user\")"
                                "(tag \"hostname\" '(\"host_0\"))"
                                "(range 1464710340000000000 1464720460000000000)"
                                "(groupby 10m avg))");
      auto tagRes = mMetaData.QueryValues("hostname", {"host_0"});
      // auto kvRes = mMetaData.GetIndex("usage_user", {{"hostname", {"host_0"}}, {"service", {"9"}}});
      auto kvRes = mMetaData.GetIndex("usage_user", {{"hostname", {"host_0"}}});
      // auto tagRes = mMetaData.QueryValues("hostname", {"host_0"});
      assert(kvRes.has_value());
      // mLogger->warn("tag; {}", fmt::join(tagRes.value(), ", "));
      mLogger->warn("result tag: {}", kvRes.value());

      // // std::optional<std::uint64_t> targetIndex = mInputs.GetIndex(queryTarget);
      // std::optional<std::uint64_t> targetIndex = mMetaData.GetIndexByName(queryTarget);
      // // std::optional<std::uint64_t> targetIndex = std::nullopt;
      // // assert(targetIndex.has_value());
      // if (!targetIndex.has_value()) {
      //   mLogger->warn("Query target {} not found, skipping query test", queryTarget);
      //   mQueryLatency = mTestingHelper.Finalize();
      //   return;
      // }
      // mLogger->info("Executing query for {} (index: {})", queryTarget, targetIndex.value());

      // mTestingHelper.SetQuerying(queryTarget, filter);
      // mTestingHelper.SetQuerying(queryTarget);
      mTestingHelper.Finalize();

      mQueryStarted = true;
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
  static constexpr std::size_t bufSize{2097152}; // 2MB
  // static constexpr std::size_t bufSize{4096}; // 4KB
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
  QueryManager<bufSize> mQueryManager;
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
  // Common::MONOTONIC_TIME mEndTS{};
  std::uint64_t mIngestionLatency{0};
  std::uint64_t mQueryLatency{0};

  // InputManager mInputs;
  // TimeTree<64> mTree;
  // std::unordered_map<std::string, MetricTree> mTrees;
  // std::unordered_map<std::uint64_t, MetricTree> mTrees;

  std::vector<QueryIO> mRunningQueries;
  TestingHelper mTestingHelper;
  // std::unordered_map<std::string, std::uint64_t> mIndex;
  // std::uint64_t mIndexCounter{0};
};

#endif
