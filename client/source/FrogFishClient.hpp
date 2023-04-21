#ifndef __FROGFISH_CLIENT
#define __FROGFISH_CLIENT

#include "EventLoop.h"
#include "ManagementPort.hpp"

#include <capnp/serialize.h>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <utility>

class FrogFishClient : public EventLoop::IEventLoopCallbackHandler {
public:
  FrogFishClient(EventLoop::EventLoop& ev): mEv(ev), mManagementPort(mEv) {
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
    mManagementPort.Connect(config->get_as<std::uint16_t>("ManagementPort").value_or(0));
    mLogger->info("Started client");
  }

  // TODO management port is async, when we need id tag during ingestion
  // Multiple options:
  // - Read the file twice, first pass is collecting the required tagsets.
  //   Second pass is the actuall insertion
  // - Read the file in chunks and suspend execution while waiting for the reply
  //   Switch to next chunk when cursor has reached the end of the chunk
  // - Combo of both previous options. Read the chunk in two passes, first pass collects
  //   tag sets, second pass sends the data.
  // - Generate data packets and tag sets in one pass. Then first retrieve the ids for the tag
  //   sets which have not yet been resolved, then fill in the remaining ids for the send batches.

  void OnEventLoopCallback() {
    auto batch = mCapWrapper->words;
    if (batch.size() == 0) {
      mLogger->info("Ingestion done");
      return;
    }
    capnp::FlatArrayMessageReader message(batch);
    proto::Batch::Reader chunk = message.getRoot<proto::Batch>();
    for (const proto::Batch::Message::Reader msg : chunk.getRecordings()) {
      std::string name;
      // name = msg.getMetric();
      // name.append(",");
      auto tags = msg.getTags();
      std::for_each(tags.begin(), tags.end(), [&](proto::Tag::Reader tag) {
        name.append(std::string{tag.getName().cStr()} + "=" + tag.getValue().cStr() + ",");
      });
      mLogger->info("Ingesting: {}", name);
      for (const proto::Batch::Message::Measurement::Reader measurement : msg.getMeasurements()) {
        // name.append(measurement.getName());

        // ++mIngestionCounter;

        // mWriter.Insert(name, msg.getTimestamp(), measurement.getValue());

        // name.resize(name.size() - measurement.getName().size());
      }
    }

  }
  
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

  bool mStarted{false};

  // capnp related parameters
  std::unique_ptr<CapnpWrapper> mCapWrapper;
  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __FROGFRISH_CLIENT
