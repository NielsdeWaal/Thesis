#ifndef __INGESTION_PORT
#define __INGESTION_PORT

#include "IngestionProtocol/proto.capnp.h"
#include "StreamSocket.h"
#include "TSC.h"
#include "UDPSocket.h"
#include "WebSocketServer.hpp"
#include "Writer.hpp"

#include <capnp/common.h>
#include <capnp/serialize.h>

template<std::size_t bufSize>
class IngestionPort
// : public Common::IStreamSocketServerHandler
// , public Common::IStreamSocketHandler
: public FrogFish::IWebSocketServerHandler
, public FrogFish::IWebSocketHandler {
public:
  IngestionPort(EventLoop::EventLoop& ev, Writer<bufSize>& writer): /*mSocket(ev, this),*/ mWriter(writer), mWebSocket(ev, this) {
    mLogger = ev.RegisterLogger("IngestionPort");
  }

  void Configure(std::uint16_t port) {
    // mSocket.StartListening(nullptr, port);
    // mSocket.BindAndListen(port);
    mWebSocket.Configure(port);
  }

  void OnConnected() final {}
  // void OnDisconnect([[maybe_unused]] Common::StreamSocket* conn) final {}
  void OnDisconnect([[maybe_unused]] websocketpp::connection_hdl conn) final {}

  // Common::IStreamSocketHandler* OnIncomingConnection() {
  FrogFish::IWebSocketHandler* OnIncomingConnection() {
    mLogger->info("New incoming client");
    return static_cast<FrogFish::IWebSocketHandler*>(this);
  }

  // void OnIncomingData(Common::StreamSocket* conn, char* data, size_t len) override {
  void OnIncomingData([[maybe_unused]] websocketpp::connection_hdl conn, FrogFish::ServerSocket::message_ptr data) final {
    // mLogger->info("Received udp frame: {}", std::string{data, len});
    auto buf = std::aligned_alloc(512, data->get_payload().size());
    std::memcpy(buf, data->get_payload().data(), data->get_payload().size());
    auto pdata = kj::arrayPtr(( const capnp::word* ) buf, data->get_payload().size() / sizeof(capnp::word));
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
    mLogger->info(
        "Ingested {} ({} bytes) points in {}s, rate: {}MB/s / {} points/sec",
        ingestCount,
        data->get_payload().size(),
        timeTakenS,
        dataRate,
        rateS);

    ::capnp::MallocMessageBuilder response;
    proto::InsertionResponse::Builder resp = response.initRoot<proto::InsertionResponse>();

    auto encodedArray = capnp::messageToFlatArray(response);
    auto encodedArrayPtr = encodedArray.asChars();
    auto encodedCharArray = encodedArrayPtr.begin();
    auto size = encodedArrayPtr.size();

    // conn->Send(encodedCharArray, size);
    mWebSocket.Send(conn, encodedCharArray, size);
  }

private:
  // Common::UDPSocket mSocket;
  // Common::StreamSocketServer mSocket;
  Writer<bufSize>& mWriter;

  FrogFish::WebSocket mWebSocket;

  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __INGESTION_PORT
