#ifndef __MANAGEMENT_PORT
#define __MANAGEMENT_PORT

#include "InfluxParser.hpp"
#include "IngestionProtocol/proto.capnp.h"
#include "MetaData.hpp"
#include "QueryManager.hpp"
#include "StreamSocket.h"
#include "TCPSocket.h"

#include <capnp/common.h>
#include <capnp/serialize.h>
#include <cstdint>

template<std::size_t bufSize>
class ManagementPort
: public Common::ITcpServerSocketHandler
, public Common::ITcpSocketHandler {
public:
  ManagementPort(EventLoop::EventLoop& ev, MetaData& metadata , QueryManager<bufSize>& queryManager)
  : mSocket(ev, this)
  , mEv(ev)
  , mMetadata(metadata)
  , mQueryManager(queryManager) {
  // {
    mSocket.BindAndListen(8080);
    mLogger = mEv.RegisterLogger("ManagementPort");
  }

  Common::ITcpSocketHandler* OnIncomingConnection() override {
    return static_cast<Common::ITcpSocketHandler*>(this);
  }

  void OnConnected() final {}

  void OnDisconnect([[maybe_unused]] Common::TcpSocket* conn) final {}

  void OnIncomingData([[maybe_unused]] Common::TcpSocket* conn, char* data, std::size_t len) final {
    auto buf = std::aligned_alloc(512, len);
    std::memcpy(buf, data, len);
    auto pdata = kj::arrayPtr(( const capnp::word* ) buf, len / sizeof(capnp::word));
    capnp::FlatArrayMessageReader msg{pdata};
    // proto::IdRequest::Reader managementMsg = msg.getRoot<proto::IdRequest>();
    proto::ManagementMessage::Reader managementMsg = msg.getRoot<proto::ManagementMessage>();

    proto::ManagementMessage::Type::Reader type = managementMsg.getType();
    switch (type.which()) {
      case proto::ManagementMessage::Type::ID_REQUEST: {
        proto::IdRequest::Reader req = type.getIdRequest();
        mLogger->info("Received id request message, {} (id: {})", req.getMetric().cStr(), req.getIdentifier());
        // std::string name;
        // name.reserve(255);

        // auto tags = managementMsg.getTagSet();
        // std::for_each(tags.begin(), tags.end(), [&](proto::Tag::Reader tag) {
        //   name.append(std::string{tag.getName().cStr()} + "=" + tag.getValue().cStr() + ",");
        // });
        // name.append(managementMsg.getMetric().cStr());

        // std::uint64_t index = mIndex.InsertSeries(name);
        // std::optional<std::uint64_t> index = mIndex.GetIndex(name);
        // if(!index.has_value()) {
        //   index = mIndex.InsertSeries(name);
        // }

        std::uint64_t index{mMetadata.GetIndex(req)};
        ::capnp::MallocMessageBuilder response;
        proto::IdResponse::Builder res = response.initRoot<proto::IdResponse>();
        res.setSetId(index);
        res.setIdentifier(req.getIdentifier());

        auto encodedArray = capnp::messageToFlatArray(response);
        auto encodedArrayPtr = encodedArray.asChars();
        auto encodedCharArray = encodedArrayPtr.begin();
        auto size = encodedArrayPtr.size();

        conn->Send(encodedCharArray, size);
        break;
      }
      case proto::ManagementMessage::Type::QUERY: {
        proto::QueryMessage::Reader query = type.getQuery();
        mLogger->info("Received query: {}", query.getQuery().cStr());
        mQueryManager.SubmitQuery(conn, query.getQuery().cStr());
        // mQueryManager.SubmitQuery("(->>"
        //                           "(index 39747)"
        //                           "(range 1452120960000000000 1452123510000000000)");
        // "(where (and (< #TS 1451621760000000000) (> #V 95))))");
      }
    }


    // conn->Shutdown();
  }

private:
  Common::TcpSocketServer mSocket;
  EventLoop::EventLoop& mEv;

  MetaData& mMetadata;
  QueryManager<bufSize>& mQueryManager;

  std::shared_ptr<spdlog::logger> mLogger;
};

#endif // __MANAGEMENT_PORT
