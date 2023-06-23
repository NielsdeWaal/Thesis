#ifndef __WEBSOCKET_SERVER
#define __WEBSOCKET_SERVER

#include "EventLoop.h"

#include <map>
#include <functional>
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

namespace FrogFish {

  using ServerSocket = websocketpp::server<websocketpp::config::asio>;

  class IWebSocketHandler {
  public:
    virtual void OnConnected() = 0;
    virtual void OnDisconnect([[maybe_unused]] websocketpp::connection_hdl conn) = 0;
    virtual void OnIncomingData([[maybe_unused]] websocketpp::connection_hdl conn, ServerSocket::message_ptr msg) = 0;
    virtual ~IWebSocketHandler() {}
  };

  class IWebSocketServerHandler {
  public:
    virtual IWebSocketHandler* OnIncomingConnection() = 0;
    virtual ~IWebSocketServerHandler() {}
  };

  class WebSocket: public EventLoop::IEventLoopCallbackHandler {
  public:
    WebSocket(EventLoop::EventLoop& ev, IWebSocketServerHandler* serverHandler): mHandler(serverHandler), mEv(ev) {
      mLogger = mEv.RegisterLogger("WebsocketServer");
      mEv.RegisterCallbackHandler(this, EventLoop::EventLoop::LatencyType::Low);
    }

    void Configure(std::size_t port) {
      mSocket.set_error_channels(websocketpp::log::elevel::all);
      mSocket.set_access_channels(websocketpp::log::alevel::none);

      mSocket.init_asio();

      mSocket.set_open_handler([this](websocketpp::connection_hdl conn) {
        mHandlers[conn] = mHandler->OnIncomingConnection();
        // mLogger->debug("New connection");
      });

      mSocket.set_message_handler([this](auto&& ph1, auto&& ph2) { this->message_handler(ph1, ph2); });

      mSocket.listen(port);
      mSocket.start_accept();
      mLogger->info("Started listening on port {}", port);
    }

    void message_handler(websocketpp::connection_hdl hdl, ServerSocket::message_ptr msg) {
      // mLogger->info("Incoming data");
      mHandlers[hdl]->OnIncomingData(hdl, msg);
    }

    void OnEventLoopCallback() final {
      mSocket.poll_one();
    }

    void Send(websocketpp::connection_hdl conn, const char* data, const std::size_t len) {
      // mSocket.send(conn, std::string{data, len}, websocketpp::frame::opcode::text);
      mSocket.send(conn, data, len, websocketpp::frame::opcode::binary);
    }

  private:
    ServerSocket mSocket;
    IWebSocketServerHandler* mHandler;
    std::map<websocketpp::connection_hdl, IWebSocketHandler*, std::owner_less<websocketpp::connection_hdl>>
        mHandlers;

    EventLoop::EventLoop& mEv;
    std::shared_ptr<spdlog::logger> mLogger;
  };

} // namespace FrogFish


#endif
