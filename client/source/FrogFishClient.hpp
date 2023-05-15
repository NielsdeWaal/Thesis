#ifndef __FROGFISH_CLIENT
#define __FROGFISH_CLIENT

#include "EventLoop.h"
#include "ManagementPort.hpp"
#include "UDPSocket.h"

#include <capnp/serialize.h>
#include <chrono>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <utility>

class FrogFishClient
: public EventLoop::IEventLoopCallbackHandler
, public Common::IUDPSocketHandler {
public:
  FrogFishClient(EventLoop::EventLoop& ev): mEv(ev), mManagementPort(mEv), mDataPort(ev, this) {
    mLogger = mEv.RegisterLogger("FrogFishClient");
    mEv.RegisterCallbackHandler(this, EventLoop::EventLoop::LatencyType::Low);
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
    if (mDataBatches.empty() && mTaggedDataBatches.empty()) {
      auto batch = mCapWrapper->words;
      using namespace std::chrono_literals;
      if (batch.size() == 0) {
        mLogger->info("Ingestion done");
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
    if (mManagementPort.RequestsDone() && (!mDataBatches.empty() || !mTaggedDataBatches.empty())) {
      // mLogger->info("Got all tag ids");
      // mWaiting = false;

      if (!mDataBatches.empty()) {
        mLogger->info("Sending {} tags", mDataBatches.size());
        // ::capnp::MallocMessageBuilder ingestMessage;
        // proto::InsertionBatch::Builder batch =
        //     ingestMessage.initRoot<proto::InsertionBatch>();
        // ::capnp::List<proto::InsertionBatch::Message>::Builder messages =
        //     batch.initRecordings(mDataBatches.size());

        // for (std::pair<
        //          std::unordered_map<
        //              std::string, std::vector<std::pair<
        //                               std::uint64_t, std::int64_t>>>::iterator,
        //          ::capnp::List<
        //              proto::InsertionBatch::Message>::Builder::Iterator>
        //          i(mDataBatches.begin(), messages.begin());
        //      i.first != mDataBatches.end(); ++i.first, ++i.second) {
        for (auto [k, v] : mDataBatches) {
          ::capnp::MallocMessageBuilder ingestMessage;
          proto::InsertionBatch::Builder batch = ingestMessage.initRoot<proto::InsertionBatch>();
          ::capnp::List<proto::InsertionBatch::Message>::Builder messages = batch.initRecordings(1);
          // ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder
          //     measurements = i.second->initMeasurements(i.first->second.size());
          ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder measurements =
              messages[0].initMeasurements(v.size());
          messages[0].setTag(mManagementPort.GetTagForName(k));

          for (std::pair<
                   std::vector<std::pair<std::uint64_t, std::int64_t>>::iterator,
                   ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder::Iterator>
                   j(v.begin(), measurements.begin());
               j.first != v.end();
               ++j.first, ++j.second) {
            j.second->setTimestamp(j.first->first);
            j.second->setValue(j.first->second);
          }
          auto encodedArray = capnp::messageToFlatArray(ingestMessage);
          auto encodedArrayPtr = encodedArray.asChars();
          auto encodedCharArray = encodedArrayPtr.begin();
          auto size = encodedArrayPtr.size();

          // mLogger->info("Sending request, size: {}, vec size: {}", size, v.size());
          mDataPort.Send(encodedCharArray, size, "127.0.0.1", 1337);
        }

        // auto encodedArray = capnp::messageToFlatArray(ingestMessage);
        // auto encodedArrayPtr = encodedArray.asChars();
        // auto encodedCharArray = encodedArrayPtr.begin();
        // auto size = encodedArrayPtr.size();

        // mLogger->info("Sending request, size: {}", size);
        // mSocket.Send(encodedCharArray, size);
        mDataBatches.clear();
        return;
      } else {
        // mLogger->info("Data batch TODO");
        for (auto [k, v] : mTaggedDataBatches) {
          ::capnp::MallocMessageBuilder ingestMessage;
          proto::InsertionBatch::Builder batch = ingestMessage.initRoot<proto::InsertionBatch>();
          ::capnp::List<proto::InsertionBatch::Message>::Builder messages = batch.initRecordings(1);
          // ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder
          //     measurements = i.second->initMeasurements(i.first->second.size());
          ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder measurements =
              messages[0].initMeasurements(v.size());
          messages[0].setTag(k);

          for (std::pair<
                   std::vector<std::pair<std::uint64_t, std::int64_t>>::iterator,
                   ::capnp::List<proto::InsertionBatch::Message::Measurement>::Builder::Iterator>
                   j(v.begin(), measurements.begin());
               j.first != v.end();
               ++j.first, ++j.second) {
            j.second->setTimestamp(j.first->first);
            j.second->setValue(j.first->second);
          }
          auto encodedArray = capnp::messageToFlatArray(ingestMessage);
          auto encodedArrayPtr = encodedArray.asChars();
          auto encodedCharArray = encodedArrayPtr.begin();
          auto size = encodedArrayPtr.size();

          // mLogger->info("Sending request, size: {}, vec size: {}", size, v.size());
          mDataPort.Send(encodedCharArray, size, "127.0.0.1", 1337);
        }

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

  void OnIncomingData([[maybe_unused]] char* data, [[maybe_unused]] std::size_t len) final {}

private:
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

  EventLoop::EventLoop& mEv;
  ManagementPort mManagementPort;
  Common::UDPSocket mDataPort;

  bool mWaiting{false};
  std::unordered_map<std::string, std::vector<std::pair<std::uint64_t, std::int64_t>>> mDataBatches;
  std::unordered_map<std::uint64_t, std::vector<std::pair<std::uint64_t, std::int64_t>>> mTaggedDataBatches;

  // capnp related parameters
  std::unique_ptr<CapnpWrapper> mCapWrapper;
  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __FROGFRISH_CLIENT
