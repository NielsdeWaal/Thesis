#ifndef __FROGFISH_CLIENT
#define __FROGFISH_CLIENT

#include "EventLoop.h"
#include "ManagementPort.hpp"
#include "TSC.h"
#include "UDPSocket.h"
// #include "WebSocket.hpp"

#include <capnp/serialize.h>
#include <chrono>
#include <cstddef>
#include <memory>
#include <numeric>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <utility>
#include <set>

// Move processing to following files:
// - write raw
// - write preprocessed
// - write influx?

// Generalize benchmarking to be able to work with all file types

class FrogFishClient
: public EventLoop::IEventLoopCallbackHandler
, public Common::IStreamSocketHandler
// , public FrogFish::IWebSocketHandler
{
public:
  FrogFishClient(EventLoop::EventLoop& ev)
  : mEv(ev)
  , mManagementPort(mEv)
  , mDataPort(ev, this)
  // , mWebSocket(ev, this)
  {
    mLogger = mEv.RegisterLogger("FrogFishClient");
    mEv.RegisterCallbackHandler(this, EventLoop::EventLoop::LatencyType::Low);

    using namespace Common::literals;
    mPrintTimer = EventLoop::EventLoop::Timer(1_s, EventLoop::EventLoop::TimerType::Repeating, [&] {
      // std::uint64_t ingestCount = 10000;
      // auto duration = Common::MONOTONIC_CLOCK::ToNanos(Common::MONOTONIC_CLOCK::Now() - mSendTS);
      // std::vector<std::uint64_t> durations;
      // durations.resize(mStats.size());
      // std::transform(mStats.begin(), mStats.end(), durations.begin(), [&](Stats stat) {
      //   return Common::MONOTONIC_CLOCK::ToNanos(stat.mConfirmTS - stat.mSendSize);
      // });

      // double rate = std::inner_product(
      //     mStats.begin(),
      //     mStats.end(),
      //     durations.begin(),
      //     0,
      //     std::plus<>(),
      //     [](Stats stat, std::uint64_t duration) {
      //       double time = duration / 1000000000.;
      //       double rate = stat.mSendCount / time;
      //       return rate;
      //     });
      // double avgRate = rate / durations.size();

      // // double timeTakenS = duration / 1000000000.;
      // // double rateS = mSendCount / timeTakenS;
      // double dataRate = (avgRate * 128) / 1000000;
      // mLogger->info("Avg: {}MB/s / {} points/sec", dataRate, avgRate);
      // mLogger->info(
      //     "Ingested {} ({} bytes) points in {}s, rate: {}MB/s / {} points/sec",
      //     mSendCount,
      //     mSendSize,
      //     timeTakenS,
      //     dataRate,
      //     rateS);
      // mSendTS = Common::MONOTONIC_CLOCK::Now();
      // mSendSize = 0;
      // mSendCount = 0;
      //   for(const auto& stat : mStats) {
      //     auto duration = Common::MONOTONIC_CLOCK::ToNanos(stat.mConfirmTS - stat.mSendTS);
      //     double timeTakenS = duration / 1000000000.;
      //     double rateS = stat.mSendCount / timeTakenS;
      //     double dataRate = (rateS * 16) / 1000000;
      //     mLogger->info(
      //         "{}: Ingested {} ({} bytes) points in {}s, rate: {}MB/s / {} points/sec",
      //         stat.mRuntime,
      //         stat.mSendCount,
      //         stat.mSendSize,
      //         timeTakenS,
      //         dataRate,
      //         rateS);
      //     }
      //   // mStats.clear();
      // std::size_t count = mSendCount - mPrevCount;
      std::size_t count = mSendCount;
      // Common::MONOTONIC_TIME sinceStart = Common::MONOTONIC_CLOCK::Now() - mStartTS;
      Common::MONOTONIC_TIME totalEncoding = std::accumulate(mEncodingDelay.begin(), mEncodingDelay.end(), 0);
      Common::MONOTONIC_TIME took = Common::MONOTONIC_CLOCK::Now() - mLastTS;
      double rate = count
                    / ((Common::MONOTONIC_CLOCK::ToNanos(took) / 1000000000.)
                       - Common::MONOTONIC_CLOCK::ToNanos(totalEncoding) / 1000000000);
      double percentage =
          (Common::MONOTONIC_CLOCK::ToNanos(totalEncoding) / Common::MONOTONIC_CLOCK::ToNanos(took)) * 100;

      mLogger->info(
          "encoding raw: {}, encoding %: {}, took: {}, count: {}, rate: {}, tags: {}",
          totalEncoding,
          percentage,
          Common::MONOTONIC_CLOCK::ToNanos(took),
          count,
          rate,
          fmt::join(tagsSend,", "));
      mStats.emplace_back(Common::MONOTONIC_CLOCK::ToNanos(took), count, rate);

      mEncodingDelay.clear();
      mLastTS = Common::MONOTONIC_CLOCK::Now();
      mPrevCount = count;
      mSendCount = 0;
      tagsSend.clear();
    });
  }

  void Configure() {
    auto config = mEv.GetConfigTable("Client");
    std::string filename(config->get_as<std::string>("TestingFile").value_or(""));
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == ENOENT) {
      mLogger->error("Failed to open {}, no such file", filename);
    } else if (fd < 0) {
      mLogger->error("Failed to open {}, errno: {}", filename, fd);
    }
    mCapWrapper = std::make_unique<CapnpWrapper>(fd);

    mManagementPort.Connect(
        config->get_as<std::string>("DatabaseHost").value_or("127.0.0.1"),
        config->get_as<std::uint16_t>("ManagementPort").value_or(0));

    mSync = config->get_as<bool>("Sync").value_or(true);

    std::string mode = config->get_as<std::string>("IngestMode").value_or("preprocessed");
    if(mode == "preprocessed") {
      mMode = Mode::PREPROCESSED;
    } else if (mode == "batch") {
      mMode = Mode::BATCH;
    } else {
      mLogger->critical("Unknown ingestion mode: {}, exiting", mode);
      assert(false);
    }

    mLogger->info("Using {} mode for ingesting", mode);

    mLogger->info("Started client");
  }

  // TODO management port is async, when we need id tag during ingestion
  // Multiple options:
  // - Read the file twice, first pass is collecting the required tagsets.
  //   Second pass is the actuall insertion
  // - Read the file in chunks and suspend execution while waiting for the reply
  //   Switch to next chunk when cursor has reached the end of the chunk
  // - Combo of both previous options. Read the chunk in two passes, first pass
  // collects
  //   tag sets, second pass sends the data.
  // - Generate data packets and tag sets in one pass. Then first retrieve the
  // ids for the tag
  //   sets which have not yet been resolved, then fill in the remaining ids for
  //   the send batches.

  void OnEventLoopCallback() {
    if (mMode == Mode::BATCH) {
      ProcessBatchesFile();
    } else if (mMode == Mode::PREPROCESSED) {
      SendPreprocessedFile();
    }
  }

  void OnConnected() final {
    mLogger->info("Connected to server");
    mConnected = true;
    mEv.AddTimer(&mPrintTimer);
    // mSendTS = Common::MONOTONIC_CLOCK::Now();
  }
  void OnDisconnect([[maybe_unused]] Common::StreamSocket* conn) final {}
  // void OnDisconnect([[maybe_unused]] websocketpp::connection_hdl conn) final {}

  void OnIncomingData(
      [[maybe_unused]] Common::StreamSocket* conn,
      [[maybe_unused]] char* data,
      [[maybe_unused]] std::size_t len) final {
    // void OnIncomingData(
    //     [[maybe_unused]] websocketpp::connection_hdl conn,
    //     [[maybe_unused]] FrogFish::ClientSocket::message_ptr msg) final {
    // TODO verify data confirmation
    mLogger->debug("Go completion from DB");
    mWaiting = false;
    // mStats.back().mConfirmTS = Common::MONOTONIC_CLOCK::Now();
    mLatencies.emplace_back(Common::MONOTONIC_CLOCK::ToNanos(Common::MONOTONIC_CLOCK::Now() - mSendTS));
  }

private:
  enum class Mode : std::uint8_t { BATCH = 0, PREPROCESSED };

  struct CapnpWrapper {
    CapnpWrapper(int fd) {
      struct stat stats;
      ::fstat(fd, &stats);
      std::size_t size = stats.st_size;
      void* data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
      if (data == MAP_FAILED) {
        spdlog::critical("Failed to mmap");
      }
      ::madvise(data, size, MADV_SEQUENTIAL);

      words = kj::ArrayPtr<capnp::word>(reinterpret_cast<capnp::word*>(data), size / sizeof(capnp::word));
    }

    kj::ArrayPtr<capnp::word> words;
  };

  void SendPreprocessedFile() {
    if (!mConnected && !mConnectionInProg) {
      mDataPort.Connect("127.0.0.1", 1337);
      mConnectionInProg = true;
      return;
    } else if (mConnected && !mWaiting) {
      kj::ArrayPtr<capnp::word> batch = mCapWrapper->words;
      using namespace std::chrono_literals;
      if (batch.size() == 0) {
        mLogger->info("Ingestion done, ingested: {} in {} sec", mTotalSend, Common::MONOTONIC_CLOCK::ToNanos(Common::MONOTONIC_CLOCK::Now() - mStartTS) / 1000000000.);

        std::ofstream output("res.csv");
        output << "time period,count period,rate period\n";
        for (const auto& tup : mStats) {
          mLogger->info("res: {},{},{}", std::get<0>(tup), std::get<1>(tup), std::get<2>(tup));
          output << fmt::format("{},{},{}\n", std::get<0>(tup), std::get<1>(tup), std::get<2>(tup));
        }
        output.flush();

        std::ofstream lFile("latencies.csv");
        lFile << "latency\n";
        for (const std::uint64_t latency : mLatencies) {
          lFile << fmt::format("{}\n", latency);
        }
        lFile.flush();

        exit(0);
        return;
      }
      capnp::FlatArrayMessageReader message(batch);
      ::capnp::MallocMessageBuilder builder;
      ::capnp::initMessageBuilderFromFlatArrayCopy(batch, builder);
      auto chunk = builder.getRoot<proto::InsertionBatch>();

      auto encodedArray = capnp::messageToFlatArray(builder);
      auto encodedArrayPtr = encodedArray.asChars();
      auto encodedCharArray = encodedArrayPtr.begin();
      auto size = encodedArrayPtr.size();
      mDataPort.Send(encodedCharArray, size);
      mSendTS = Common::MONOTONIC_CLOCK::Now();
      mLogger->trace("Sending data size: {}", size);
      mWaiting = mSync;

      auto reader = chunk.asReader();
      for (const auto& rec : reader.getRecordings()) {
        auto measurements = rec.getMeasurements();
        mTotalSend += measurements.size();
        mSendCount += measurements.size();
        tagsSend.insert(rec.getTag());
      }

      mCapWrapper->words = kj::arrayPtr(const_cast<capnp::word*>(message.getEnd()), mCapWrapper->words.end());
    }
  }

  void ProcessBatchesFile() {
    if (mDataBatches.empty() && mTaggedDataBatches.empty()) {
      auto batch = mCapWrapper->words;
      using namespace std::chrono_literals;
      if (batch.size() == 0) {
        mLogger->info("Ingestion done");

        std::ofstream output("res.csv");
        output << "time period,count period,rate period\n";
        for (const auto& tup : mStats) {
          mLogger->info("res: {},{},{}", std::get<0>(tup), std::get<1>(tup), std::get<2>(tup));
          output << fmt::format("{},{},{}\n", std::get<0>(tup), std::get<1>(tup), std::get<2>(tup));
        }
        // // fmt::format_to(output, "nr. points,bytes,time taken,mbs,ps");
        // output << "runtime,nr. points,bytes,time taken,mbs,ps\n";
        // for(const Stats& stat : mStats) {
        //   auto duration = Common::MONOTONIC_CLOCK::ToNanos(stat.mConfirmTS - stat.mSendTS);
        //   double timeTakenS = duration / 1000000000.;
        //   double rateS = stat.mSendCount / timeTakenS;
        //   double dataRate = (rateS * 16) / 1000000;
        //   output << fmt::format("{},{},{},{},{},{}\n", stat.mRuntime, stat.mSendCount, stat.mSendSize, timeTakenS,
        //   dataRate, rateS);
        // }
        output.flush();

        exit(0);
        return;
      }
      capnp::FlatArrayMessageReader message(batch);
      proto::Batch::Reader chunk = message.getRoot<proto::Batch>();
      for (const proto::Batch::Message::Reader msg : chunk.getRecordings()) {
        // mLogger->info("Ingesting: {}", name);
        for (const proto::Batch::Message::Measurement::Reader measurement : msg.getMeasurements()) {
          std::uint64_t index = mManagementPort.GetTagSetId(msg, measurement);
          if (index == 0) {
            std::string tagSet;
            tagSet.reserve(255);
            ::capnp::List<::proto::Tag, ::capnp::Kind::STRUCT>::Reader tags = msg.getTags();
            std::for_each(tags.begin(), tags.end(), [&](proto::Tag::Reader tag) {
              tagSet.append(std::string{tag.getName().cStr()} + "=" + tag.getValue().cStr() + ",");
            });
            tagSet.append(measurement.getName());
            mDataBatches[tagSet].push_back({msg.getTimestamp(), measurement.getValue()});
            // mWaiting = true;
          } else {
            // mLogger->info("Known tag");
            mTaggedDataBatches[index].push_back({msg.getTimestamp(), measurement.getValue()});
          }
          // name.append(measurement.getName());
          // std::this_thread::sleep_for(1000ms);

          // ++mIngestionCounter;

          // mWriter.Insert(name, msg.getTimestamp(), measurement.getValue());

          // name.resize(name.size() - measurement.getName().size());
        }
      }
      mCapWrapper->words = kj::arrayPtr(const_cast<capnp::word*>(message.getEnd()), mCapWrapper->words.end());
    }
    if (mManagementPort.RequestsDone() && !mConnected && !mConnectionInProg) {
      mDataPort.Connect("127.0.0.1", 1337);
      // mWebSocket.Configure("ws://127.0.0.1", 1337);
      mConnectionInProg = true;
      return;
    }
    if (mManagementPort.RequestsDone() && (!mDataBatches.empty() || !mTaggedDataBatches.empty()) && !mWaiting
        && mConnected) {
      // mLogger->info("Got all tag ids");
      mWaiting = false;

      if (!mDataBatches.empty()) {
        mLogger->info("Sending {} tags", mDataBatches.size());
        ::capnp::MallocMessageBuilder ingestMessage;
        proto::InsertionBatch::Builder batch = ingestMessage.initRoot<proto::InsertionBatch>();
        ::capnp::List<proto::InsertionBatch::Message>::Builder messages = batch.initRecordings(mDataBatches.size());

        for (std::pair<
                 std::unordered_map<std::string, std::vector<std::pair<std::uint64_t, std::int64_t>>>::iterator,
                 ::capnp::List<proto::InsertionBatch::Message>::Builder::Iterator>
                 i(mDataBatches.begin(), messages.begin());
             i.first != mDataBatches.end();
             ++i.first, ++i.second) {
          // for (auto [k, v] : mDataBatches) {
          //   ::capnp::MallocMessageBuilder ingestMessage;
          //   proto::InsertionBatch::Builder batch = ingestMessage.initRoot<proto::InsertionBatch>();
          //   ::capnp::List<proto::InsertionBatch::Message>::Builder messages = batch.initRecordings(1);
          //   // ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder
          //   //     measurements = i.second->initMeasurements(i.first->second.size());
          ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder measurements =
              i.second->initMeasurements(i.first->second.size());
          i.second->setTag(mManagementPort.GetTagForName(i.first->first));

          for (std::pair<
                   std::vector<std::pair<std::uint64_t, std::int64_t>>::iterator,
                   ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder::Iterator>
                   j(i.first->second.begin(), measurements.begin());
               j.first != i.first->second.end();
               ++j.first, ++j.second) {
            j.second->setTimestamp(j.first->first);
            j.second->setValue(j.first->second);
          }
          //   auto encodedArray = capnp::messageToFlatArray(ingestMessage);
          //   auto encodedArrayPtr = encodedArray.asChars();
          //   auto encodedCharArray = encodedArrayPtr.begin();
          //   auto size = encodedArrayPtr.size();

          //   // mLogger->info("Sending request, size: {}, vec size: {}", size, v.size());
          //   // mDataPort.Send(encodedCharArray, size, "127.0.0.1", 1337);
          //   mDataPort.Send(encodedCharArray, size);
          //   // mWebSocket.Send(encodedCharArray, size);
        }

        auto encodedArray = capnp::messageToFlatArray(ingestMessage);
        auto encodedArrayPtr = encodedArray.asChars();
        auto encodedCharArray = encodedArrayPtr.begin();
        auto size = encodedArrayPtr.size();
        mDataPort.Send(encodedCharArray, size);

        // mLogger->info("Sending request, size: {}", size);
        // mSocket.Send(encodedCharArray, size);
        mWaiting = mSync;
        mDataBatches.clear();
        return;
      } else {
        // mLogger->info("databatches size: {}", mTaggedDataBatches.size());

        // mStats.push_back({});

        ::capnp::MallocMessageBuilder ingestMessage;
        proto::InsertionBatch::Builder batch = ingestMessage.initRoot<proto::InsertionBatch>();
        ::capnp::List<proto::InsertionBatch::Message>::Builder messages =
            batch.initRecordings(mTaggedDataBatches.size());

        // for (auto [tag, values] : mTaggedDataBatches) {
        auto startEncode = Common::MONOTONIC_CLOCK::Now();
        std::uint64_t count{0};
        for (std::pair<
                 std::unordered_map<std::uint64_t, std::vector<std::pair<std::uint64_t, std::int64_t>>>::iterator,
                 ::capnp::List<proto::InsertionBatch::Message>::Builder::Iterator>
                 msgs(mTaggedDataBatches.begin(), messages.begin());
             msgs.first != mTaggedDataBatches.end();
             ++msgs.first, ++msgs.second) {
          // ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder
          //     measurements = i.second->initMeasurements(i.first->second.size());
          ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder measurements =
              msgs.second->initMeasurements(msgs.first->second.size());
          msgs.second->setTag(msgs.first->first);

          for (std::pair<
                   std::vector<std::pair<std::uint64_t, std::int64_t>>::iterator,
                   ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder::Iterator>
                   j(msgs.first->second.begin(), measurements.begin());
               j.first != msgs.first->second.end();
               ++j.first, ++j.second) {
            j.second->setTimestamp(j.first->first);
            j.second->setValue(j.first->second);
            ++count;
          }
        }
        auto encodedArray = capnp::messageToFlatArray(ingestMessage);
        auto encodedArrayPtr = encodedArray.asChars();
        auto encodedCharArray = encodedArrayPtr.begin();
        auto size = encodedArrayPtr.size();
        mEncodingDelay.emplace_back(Common::MONOTONIC_CLOCK::Now() - startEncode);

        // auto segments = ingestMessage.getSegmentsForOutput();

        // for (const kj::ArrayPtr<const capnp::word>& segment : segments) {
        //   mLogger->info("Sending request, size: {}", segment.size());
        //   mDataPort.Send(segment.asChars().begin(), segment.size());
        // }

        // mDataPort.Send(encodedCharArray, size, "127.0.0.1", 1337);

        mDataPort.Send(encodedCharArray, size);
        // mLogger->info("Sending data size: {}", size);

        // mWebSocket.Send(encodedCharArray, size);
        // mSendTS = Common::MONOTONIC_CLOCK::Now();
        // mSendSize += size;
        mSendCount += count;
        // mStats.back().mSendTS = Common::MONOTONIC_CLOCK::Now();
        // mStats.back().mSendCount += 10000;
        // mStats.back().mSendSize += size;
        // mStats.push_back({.mSendTS = Common::MONOTONIC_CLOCK::Now(), .mSendCount = 10000, .mSendSize = size,
        // .mRuntime = Common::MONOTONIC_CLOCK::Now() - mStartTS}); mWaiting = true;
        mWaiting = mSync;

        mTaggedDataBatches.clear();
        return;
      }

      if (mDataBatches.empty() && mTaggedDataBatches.empty()) {
        mLogger->info("Send all data to DB, read more data");
        // mWaiting = false;
        // const_cast<capnp::word*>(message.getEnd())
        // mCapWrapper->words = kj::arrayPtr(end, mCapWrapper->words.end());
      }

      // TODO push capnp offset
    }
  }

  EventLoop::EventLoop& mEv;
  ManagementPort mManagementPort;
  Common::StreamSocket mDataPort;

  // FrogFish::WebSocket mWebSocket;

  bool mWaiting{false};
  bool mConnectionInProg{false};
  bool mConnected{false};
  std::unordered_map<std::string, std::vector<std::pair<std::uint64_t, std::int64_t>>> mDataBatches;
  std::unordered_map<std::uint64_t, std::vector<std::pair<std::uint64_t, std::int64_t>>> mTaggedDataBatches;

  // Common::MONOTONIC_TIME mSendTS{0};
  // std::size_t mSendSize{0};
  // std::size_t mSendCount{0};

  // struct Stats {
  //   Common::MONOTONIC_TIME mSendTS{0};
  //   std::size_t mSendCount{0};
  //   std::size_t mSendSize{0};
  //   Common::MONOTONIC_TIME mRuntime{0};
  //   Common::MONOTONIC_TIME mConfirmTS{0};
  // };
  // std::vector<Stats> mStats;
  std::size_t mPrevCount{0};
  std::size_t mSendCount{0};
  std::size_t mTotalSend{0};

  Common::MONOTONIC_TIME mStartTS{Common::MONOTONIC_CLOCK::Now()};
  Common::MONOTONIC_TIME mLastTS{Common::MONOTONIC_CLOCK::Now()};
  Common::MONOTONIC_TIME mSendTS{Common::MONOTONIC_CLOCK::Now()};
  std::vector<std::tuple<std::uint64_t, std::uint64_t, double>> mStats;
  std::vector<Common::MONOTONIC_TIME> mEncodingDelay;
  std::vector<std::uint64_t> mLatencies;

  EventLoop::EventLoop::Timer mPrintTimer;

  Mode mMode{Mode::PREPROCESSED};
  std::set<std::uint64_t> tagsSend;

  bool mSync{true};

  // capnp related parameters
  std::unique_ptr<CapnpWrapper> mCapWrapper;
  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __FROGFRISH_CLIENT
