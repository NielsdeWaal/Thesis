#ifndef __MANAGEMENT_PORT
#define __MANAGEMENT_PORT

#include "IngestionProtocol/proto.capnp.h"
#include "StreamSocket.h"

#include <capnp/common.h>
#include <capnp/serialize.h>
#include <kj/common.h>
#include <unordered_set>

class ManagementPort
: public Common::IStreamSocketHandler
, public EventLoop::IEventLoopCallbackHandler {
public:
  ManagementPort(EventLoop::EventLoop& ev): mEv(ev), mSocket(mEv, this) {
    mLogger = mEv.RegisterLogger("ManagementPort");
    mEv.RegisterCallbackHandler(this, EventLoop::EventLoop::LatencyType::Low);
    std::srand(std::time(nullptr));
  }

  void Connect(const std::string& host, std::uint16_t port) {
    mLogger->info("Connecting to: {}", port);
    mSocket.Connect(host.c_str(), port);
  }

  void OnConnected() final {
    mLogger->info("Connected");
  }

  void OnDisconnect([[maybe_unused]] Common::StreamSocket* sock) final {
    mLogger->warn("Disconnected from management port");
  }

  void OnIncomingData([[maybe_unused]] Common::StreamSocket* sock, char* data, std::size_t len) final {
    auto buf = std::aligned_alloc(512, len);
    std::memcpy(buf, data, len);
    auto pdata = kj::arrayPtr(( const capnp::word* ) buf, len / sizeof(capnp::word));
    capnp::FlatArrayMessageReader msg{pdata};
    proto::IdResponse::Reader managementMsg = msg.getRoot<proto::IdResponse>();

    mLogger->info(
        "Got response for request: {} ({}), id: {}",
        managementMsg.getIdentifier(),
        mRequestMapping[managementMsg.getIdentifier()],
        managementMsg.getSetId());

    mSetMapping[mRequestMapping[managementMsg.getIdentifier()]] = managementMsg.getSetId();
    mInprogressRequests.erase(mRequestMapping[managementMsg.getIdentifier()]);
    mRequestMapping.erase(managementMsg.getIdentifier());

    if(mInprogressRequests.empty()) {
      mLogger->info("No more in progress requests");
    } else {
      mLogger->info("Still in progress: {}", fmt::join(mInprogressRequests, ",\n "));
    }
    mWaitingForResponse = false;
  }

  std::uint64_t GetTagSetId(
      const proto::Batch::Message::Reader msg,
      const proto::Batch::Message::Measurement::Reader measurement) {
    // TODO move this to something like "CreateCononicalName()"
    std::string tagSet;
    tagSet.reserve(255);
    ::capnp::List<::proto::Tag, ::capnp::Kind::STRUCT>::Reader tags = msg.getTags();
    std::for_each(tags.begin(), tags.end(), [&](proto::Tag::Reader tag) {
      tagSet.append(std::string{tag.getName().cStr()} + "=" + tag.getValue().cStr() + ",");
    });
    tagSet.append(measurement.getName());

    if (mInprogressRequests.contains(tagSet)) {
      return 0;
    }

    if (!mSetMapping.contains(tagSet)) {
      // mQueue.push_back({encodedCharArray, size});
      int requestId = std::rand();
      mLogger->warn("{} is not yet registered with server, sending request with id: {}", tagSet, requestId);

      std::vector<std::pair<std::string, std::string>> reqTags;
      std::for_each(tags.begin(), tags.end(), [&](proto::Tag::Reader tag) {
        reqTags.push_back({std::string{tag.getName().cStr()}, tag.getValue().cStr()});
      });

      mRequestMapping[requestId] = tagSet;

      mQueue.push_back({std::move(reqTags), measurement.getName(), requestId});
      mInprogressRequests.insert(tagSet);

      return 0;
    }

    return mSetMapping[tagSet];
  }

  void OnEventLoopCallback() {
    if (!mQueue.empty() && !mWaitingForResponse) {
      auto rq = mQueue.back();
      ::capnp::MallocMessageBuilder request;

      proto::IdRequest::Builder req = request.initRoot<proto::IdRequest>();
      req.setIdentifier(rq.requestToken);
      req.setMetric(rq.metric);

      ::capnp::List<proto::Tag>::Builder tagBuilder = req.initTagSet(rq.tags.size());
      for (std::pair<
               std::vector<std::pair<std::string, std::string>>::iterator,
               ::capnp::List<proto::Tag>::Builder::Iterator> i(rq.tags.begin(), tagBuilder.begin());
           i.first != rq.tags.end();
           ++i.first, ++i.second) {
        i.second->setName(i.first->first);
        i.second->setValue(i.first->second);
      }

      auto encodedArray = capnp::messageToFlatArray(request);
      auto encodedArrayPtr = encodedArray.asChars();
      auto encodedCharArray = encodedArrayPtr.begin();
      auto size = encodedArrayPtr.size();

      mLogger->info("Sending request, size: {}", size);

      mSocket.Send(encodedCharArray, size);
      mQueue.pop_back();

      mWaitingForResponse = true;
    }
  }

  bool RequestsDone() {
    return mQueue.empty() && !mWaitingForResponse;
  }

  std::uint64_t GetTagForName(const std::string& name) {
    return mSetMapping[name];
  }

private:
  struct RequestMessage {
    std::vector<std::pair<std::string, std::string>> tags;
    std::string metric;
    int requestToken;
  };

  EventLoop::EventLoop& mEv;
  Common::StreamSocket mSocket;

  std::unordered_map<std::string, std::uint64_t> mSetMapping;
  std::unordered_map<std::uint64_t, std::string> mRequestMapping;
  std::unordered_set<std::string> mInprogressRequests;

  std::vector<RequestMessage> mQueue;
  bool mWaitingForResponse{false};

  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __MANAGEMENT_PORT
