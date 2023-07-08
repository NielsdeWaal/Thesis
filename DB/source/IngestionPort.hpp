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
#include <kj/common.h>
#include <numeric>

template<std::size_t bufSize>
class IngestionPort
: public Common::IStreamSocketServerHandler
, public Common::IStreamSocketHandler
// : public FrogFish::IWebSocketServerHandler
// , public FrogFish::IWebSocketHandler
{
public:
  IngestionPort(EventLoop::EventLoop& ev, Writer<bufSize>& writer)
  : mSocket(ev, this)
  , mWriter(writer) /*, mWebSocket(ev, this)*/ {
    mLogger = ev.RegisterLogger("IngestionPort");
    mLogger->set_level(spdlog::level::info);
    auto config = ev.GetConfigTable("DB");
    mNoop = config->get_as<bool>("Noop").value_or(false);
  }

  void Configure(std::uint16_t port) {
    // mSocket.StartListening(nullptr, port);
    mSocket.BindAndListen(port);
    // mWebSocket.Configure(port);
    mLogger->info("Configured: port: {}, noop mode: {}", port, mNoop);
  }

  void OnConnected() final {}
  void OnDisconnect([[maybe_unused]] Common::StreamSocket* conn) final {}
  // void OnDisconnect([[maybe_unused]] websocketpp::connection_hdl conn) final {}

  Common::IStreamSocketHandler* OnIncomingConnection() {
    // FrogFish::IWebSocketHandler* OnIncomingConnection() {
    mLogger->info("New incoming client");
    // return static_cast<FrogFish::IWebSocketHandler*>(this);
    return static_cast<Common::IStreamSocketHandler*>(this);
  }

  void OnIncomingData(Common::StreamSocket* conn, char* data, size_t len) override {
    if(!mBuffers.contains(conn)) {
      mLogger->info("Registered buffers for new connection");
      mBuffers[conn] = ConnBuffer{};
    }
    mLogger->trace("Received message of size: {}", len);
    // auto res = capnp::expectedSizeInWordsFromPrefix(kj::arrayPtr(( const capnp::word* ) data, len /
    // sizeof(capnp::word))); mLogger->info("expected size: {}", res * sizeof(capnp::word));

    auto& buff = mBuffers[conn];
    if (!buff.mReceiving) {
      capnpHeader header = ProcessSegmentTable(data, len);
      auto overflow = header.totalSize <=> len;
      mLogger->debug(
          "nr segments: {}, segSize: {}, total: {}, size: {}, offset: {}, underflow: {}, overflow: {}, exact: {}",
          header.nrSegments,
          header.firstSegmentSize,
          header.totalSize,
          fmt::join(header.sizes, ", "),
          header.offset,
          overflow > 0,
          overflow < 0,
          overflow == 0);

      if (buff.mMessageBuffer.capacity() < header.totalSize) {
        buff.mMessageBuffer.reserve(header.totalSize);
      }

      // std::memcpy(mMessageBuffer.data(), data, len);
      std::size_t dataLen = std::min(len, header.totalSize);
      for (std::size_t i = 0; i < dataLen; ++i) {
        buff.mMessageBuffer.emplace_back(data[i]);
      }

      if (overflow > 0) {
        buff.mBufferOffset = len;
        buff.mReceiving = true;
        buff.mDataRemaining = header.totalSize - len;
      } else if (overflow < 0) {
        // More than one message in the buffer, recurse to process possible new message
        ProcessMessage(buff);
        buff.mMessageBuffer.clear();
        OnIncomingData(conn, data + header.totalSize, len - header.totalSize);
      } else if (overflow == 0) {
        // Received exact message
        ProcessMessage(buff);
        SendResponse(conn);
        buff.mMessageBuffer.clear();
      }
    } else if (buff.mDataRemaining > 0) {
      // std::memcpy(mMessageBuffer.data() + mBufferOffset, data, len);
      std::size_t dataLen = std::min(len, buff.mDataRemaining);
      for (std::size_t i = 0; i < dataLen; ++i) {
        buff.mMessageBuffer.emplace_back(data[i]);
      }

      bool overflow = buff.mDataRemaining < len;

      buff.mDataRemaining -= dataLen;
      buff.mBufferOffset += dataLen;
      mLogger->debug("Bytes remaining: {}, overflow: {}", buff.mDataRemaining, overflow);

      if (buff.mDataRemaining == 0) {
        mLogger->trace("Received full message, parsing...");

        ProcessMessage(buff);
        SendResponse(conn);

        buff.mMessageBuffer.clear();
        buff.mReceiving = false;
      }
    }
  }

private:
  struct capnpHeader;
  struct ConnBuffer;

  void ProcessMessage(ConnBuffer& conn) {
    const auto pdata =
        kj::arrayPtr(( const capnp::word* ) conn.mMessageBuffer.data(), conn.mMessageBuffer.size() / sizeof(capnp::word));
    capnp::FlatArrayMessageReader msg{pdata};
    proto::InsertionBatch::Reader insertMsg = msg.getRoot<proto::InsertionBatch>();

    std::vector<std::uint64_t> tags;
    tags.reserve(50);
    std::uint64_t ingestCount{0};
    Common::MONOTONIC_TIME start{Common::MONOTONIC_CLOCK::Now()};
    // mLogger->info("Received insert msg: {}", insertMsg.toString().flatten());
    // auto msgs = insertMsg.getRecordings();
    for (const proto::InsertionBatch::Message::Reader batch : insertMsg.getRecordings()) {
      // mLogger->info("Received insert for tag: {}", batch.getTag());
      const std::uint64_t tag{batch.getTag()};
      tags.emplace_back(tag);
      for (const proto::InsertionBatch::Message::Measurement::Reader meas : batch.getMeasurements()) {
        mWriter.Insert(tag, meas.getTimestamp(), meas.getValue());
        ++ingestCount;
      }
    }

    auto duration = Common::MONOTONIC_CLOCK::ToNanos(Common::MONOTONIC_CLOCK::Now() - start);
    double timeTakenS = duration / 1000000000.;
    double rateS = ingestCount / timeTakenS;
    double dataRate = (rateS * 16) / 1000000;
    mLogger->trace("Processed tags: {}", fmt::join(tags, ", "));
    mLogger->debug(
        "Ingested {} ({} bytes) points in {}s, rate: {}MB/s / {} points/sec",
        ingestCount,
        conn.mMessageBuffer.size(),
        timeTakenS,
        dataRate,
        rateS);
  }

  void SendResponse(Common::StreamSocket* conn) {
      ::capnp::MallocMessageBuilder response;
      proto::InsertionResponse::Builder resp = response.initRoot<proto::InsertionResponse>();

      auto encodedArray = capnp::messageToFlatArray(response);
      auto encodedArrayPtr = encodedArray.asChars();
      auto encodedCharArray = encodedArrayPtr.begin();
      auto size = encodedArrayPtr.size();

      conn->Send(encodedCharArray, size);
  }

  capnpHeader ProcessSegmentTable(char* data, std::size_t len) {
    capnpHeader header;
    const std::uint32_t* table = reinterpret_cast<const std::uint32_t*>(data);

    header.nrSegments = table[0] + 1;
    header.offset = (header.nrSegments / 2u + 1u) * sizeof(capnp::word);
    header.firstSegmentSize = table[1] * sizeof(capnp::word);

    for (std::uint32_t i = 1; i < header.nrSegments; ++i) {
      header.sizes.emplace_back(table[i + 1] * sizeof(capnp::word));
    }
    header.totalSize = std::accumulate(header.sizes.begin(), header.sizes.end(), 0) + header.firstSegmentSize + header.offset;

    return header;
  }

private:
  struct capnpHeader {
    std::size_t totalSize;
    std::vector<uint32_t> sizes;
    std::uint32_t nrSegments;
    std::size_t offset;
    std::uint32_t firstSegmentSize;
  };
  struct ConnBuffer {
    bool mReceiving{false};

    std::vector<char> mMessageBuffer;
    std::size_t mDataRemaining{0};
    std::size_t mBufferOffset{0};
  };
  // Common::UDPSocket mSocket;
  Common::StreamSocketServer mSocket;
  Writer<bufSize>& mWriter;
  std::unordered_map<Common::StreamSocket*, ConnBuffer> mBuffers;
  bool mNoop{false};

  // FrogFish::WebSocket mWebSocket;

  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __INGESTION_PORT
