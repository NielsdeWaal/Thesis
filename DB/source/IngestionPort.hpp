#ifndef __INGESTION_PORT
#define __INGESTION_PORT

#include "IngestionProtocol/proto.capnp.h"
#include "TSC.h"
#include "UDPSocket.h"
#include "Writer.hpp"

#include <capnp/common.h>
#include <capnp/serialize.h>

template<std::size_t bufSize> class IngestionPort: public Common::IUDPSocketHandler {
public:
  IngestionPort(EventLoop::EventLoop& ev, Writer<bufSize>& writer): mSocket(ev, this), mWriter(writer) {
    mLogger = ev.RegisterLogger("IngestionPort");
  }

  void Configure(std::uint16_t port) {
    mSocket.StartListening(nullptr, port);
  }

  void OnIncomingData([[maybe_unused]] char* data, [[maybe_unused]] size_t len) override {
    // mLogger->info("Received udp frame: {}", std::string{data, len});
    // auto buf = std::aligned_alloc(512, len);
    // std::memcpy(buf, data, len);
    auto pdata = kj::arrayPtr(( const capnp::word* ) data, len / sizeof(capnp::word));
    capnp::FlatArrayMessageReader msg{pdata};
    proto::InsertionBatch::Reader insertMsg = msg.getRoot<proto::InsertionBatch>();

    std::uint64_t ingestCount{0};
    Common::MONOTONIC_TIME start{Common::MONOTONIC_CLOCK::Now()};
    // mLogger->info("Received insert msg: {}", insertMsg.toString().flatten());
    // auto msgs = insertMsg.getRecordings();
    for (const proto::InsertionBatch::Message::Reader batch : insertMsg.getRecordings()) {
      // mLogger->info("Received insert for tag: {}", batch.getTag());
      for (const proto::InsertionBatch::Message::Measurement::Reader meas : batch.getMeasurements()) {
        mWriter.Insert(batch.getTag(), meas.getTimestamp(), meas.getValue());
        ++ingestCount;
      }
    }
    auto duration = Common::MONOTONIC_CLOCK::ToNanos(Common::MONOTONIC_CLOCK::Now() - start);
    double timeTakenS = duration / 1000000000.;
    double rateS = ingestCount / timeTakenS;
    double dataRate = (rateS * 128) / 1000000;
    // mLogger->info("Ingested {} points in {}s, rate: {}MB/s / {} points/sec", ingestCount, timeTakenS, dataRate,
    // rateS);
  }

private:
  Common::UDPSocket mSocket;
  Writer<bufSize>& mWriter;
  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __INGESTION_PORT
