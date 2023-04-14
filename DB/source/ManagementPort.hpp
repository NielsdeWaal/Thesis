#ifndef __MANAGEMENT_PORT
#define __MANAGEMENT_PORT

#include "InfluxParser.hpp"
#include "IngestionProtocol/proto.capnp.h"
#include "StreamSocket.h"

#include <capnp/common.h>
#include <capnp/serialize.h>

class ManagementPort
: public Common::IStreamSocketServerHandler
, public Common::IStreamSocketHandler {
public:
  ManagementPort(EventLoop::EventLoop& ev, InputManager& index): mSocket(ev, this), mEv(ev), mIndex(index) {
    mSocket.BindAndListen(8080);
    mLogger = mEv.RegisterLogger("ManagementPort");
  }

  Common::IStreamSocketHandler* OnIncomingConnection() override {
    return this;
  }

  void OnConnected() final {}

  void OnDisconnect([[maybe_unused]] Common::StreamSocket* conn) final {}

  void OnIncomingData([[maybe_unused]] Common::StreamSocket* conn, char* data, std::size_t len) final {
    auto buf = std::aligned_alloc(len, 512);
    std::memcpy(buf, data, len);
    auto pdata = kj::arrayPtr(( const capnp::word* ) buf, len / sizeof(capnp::word));
    capnp::FlatArrayMessageReader msg{pdata};
    proto::IdRequest::Reader managementMsg = msg.getRoot<proto::IdRequest>();

    // mLogger->info("Received management message, {} ()", managementMsg.getMetric().cStr());
    std::string name;
    name.reserve(255);

    auto tags = managementMsg.getTagSet();
    std::for_each(tags.begin(), tags.end(), [&](proto::Tag::Reader tag) {
      name.append(std::string{tag.getName().cStr()} + "=" + tag.getValue().cStr() + ",");
    });
    name.append(managementMsg.getMetric().cStr());

    std::uint64_t index = mIndex.InsertSeries(name);

    ::capnp::MallocMessageBuilder response;
    proto::IdResponse::Builder res = response.initRoot<proto::IdResponse>();
    res.setSetId(index);

    auto encodedArray = capnp::messageToFlatArray(response);
    auto encodedArrayPtr = encodedArray.asChars();
    auto encodedCharArray= encodedArrayPtr.begin();
    auto size = encodedArrayPtr.size();

    conn->Send(encodedCharArray, size);
    
    conn->Shutdown();
  }

private:
  Common::StreamSocketServer mSocket;
  EventLoop::EventLoop& mEv;

  InputManager& mIndex;

  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __MANAGEMENT_PORT
