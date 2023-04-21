#ifndef __MANAGEMENT_PORT
#define __MANAGEMENT_PORT

#include "IngestionProtocol/proto.capnp.h"
#include "StreamSocket.h"
#include <kj/common.h>
#include <capnp/common.h>
#include <capnp/serialize.h>

class ManagementPort : public Common::IStreamSocketHandler
{
public:
  ManagementPort(EventLoop::EventLoop& ev)
  : mEv(ev)
  , mSocket(mEv, this)
  {
    mLogger = mEv.RegisterLogger("ManagementPort");
    std::srand(std::time(nullptr));
  }

  void Connect(std::uint16_t port) {
    mLogger->info("Connecting to: {}", port);
    mSocket.Connect("127.0.0.1", port);
  }
  
  void OnConnected() final {
    mLogger->info("Connected");
  }

  void OnDisconnect([[maybe_unused]] Common::StreamSocket* sock) final
  {
    mLogger->warn("Disconnected from management port");
  }

  void OnIncomingData([[maybe_unused]] Common::StreamSocket* sock, char* data, std::size_t len) final {
    auto buf = std::aligned_alloc(len, 512);
    std::memcpy(buf, data, len);
    auto pdata = kj::arrayPtr(( const capnp::word* ) buf, len / sizeof(capnp::word));
    capnp::FlatArrayMessageReader msg{pdata};
    proto::IdResponse::Reader managementMsg = msg.getRoot<proto::IdResponse>();

    mLogger->info("Got response for request: {}", managementMsg.getIdentifier());
  }

  std::uint64_t GetTagSetId(const std::string& tagSet) {
    if(!mSetMapping.contains(tagSet)) {
      int requestId = std::rand();
      mLogger->warn("tagSet is not yet registered with server, sending request with id: {}", requestId);

      ::capnp::MallocMessageBuilder request;

      mRequestMapping[requestId] = tagSet;
      proto::IdRequest::Builder req = request.initRoot<proto::IdRequest>();
      req.setIdentifier(requestId);

      auto encodedArray = capnp::messageToFlatArray(request);
      auto encodedArrayPtr = encodedArray.asChars();
      auto encodedCharArray = encodedArrayPtr.begin();
      auto size = encodedArrayPtr.size();

      mSocket.Send(encodedCharArray, size);

      return 0;
    }
  }
  
private:
  EventLoop::EventLoop &mEv;
  Common::StreamSocket mSocket;

  std::unordered_map<std::string, std::uint64_t> mSetMapping;
  std::unordered_map<std::uint64_t, std::string> mRequestMapping;
  
  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __MANAGEMENT_PORT
