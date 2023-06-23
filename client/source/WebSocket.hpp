#ifndef __WEBSOCKET_SERVER
#define __WEBSOCKET_SERVER

#include "EventLoop.h"

#include <map>
#include <functional>
#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/client.hpp>

namespace FrogFish {

  using ClientSocket = websocketpp::client<websocketpp::config::asio_client>;

  class IWebSocketHandler {
  public:
    virtual void OnConnected() = 0;
    virtual void OnDisconnect([[maybe_unused]] websocketpp::connection_hdl conn) = 0;
    virtual void OnIncomingData([[maybe_unused]] websocketpp::connection_hdl conn, ClientSocket::message_ptr msg) = 0;
    virtual ~IWebSocketHandler() {}
  };

  class WebSocket: public EventLoop::IEventLoopCallbackHandler {
  public:
    WebSocket(EventLoop::EventLoop& ev, IWebSocketHandler* serverHandler): mHandler(serverHandler), mEv(ev) {
      mLogger = mEv.RegisterLogger("WebsocketClient");
    }

    void Configure(const std::string& uri, std::size_t port) {
      mSocket.set_error_channels(websocketpp::log::elevel::all);
      mSocket.set_access_channels(websocketpp::log::alevel::none);

      mSocket.init_asio();

      mSocket.set_open_handler([this]([[maybe_unused]] websocketpp::connection_hdl conn) {
        mLogger->info("Connected");
        mHandler->OnConnected();
      });

      mSocket.set_message_handler([this](auto&& ph1, auto&& ph2) { this->message_handler(ph1, ph2); });

      std::string addr =  fmt::format("{}:{}", uri, port);

      mLogger->info("Connecting to: {}", addr);

      websocketpp::lib::error_code ec;
      con = mSocket.get_connection(addr, ec);
      if(ec) {
        mLogger->error("Failed to connect to websocket, {}", ec.message());
      }

      mSocket.connect(con);

      mLogger->info("Started connection to {} on port {}", uri, port);
      mEv.RegisterCallbackHandler(this, EventLoop::EventLoop::LatencyType::Low);
    }

    void message_handler(websocketpp::connection_hdl hdl, ClientSocket::message_ptr msg) {
      // mLogger->info("Incoming data");
      mHandler->OnIncomingData(hdl, msg);
    }

    void OnEventLoopCallback() final {
      mSocket.poll_one();
    }

    void Send(const char* data, const std::size_t len) {
      // mLogger->info("Sending data to server");
      // mSocket.send(std::string{data, len}, websocketpp::frame::opcode::text);
      con->send(data, len);
    }

  private:
    ClientSocket mSocket;
    IWebSocketHandler* mHandler;

    ClientSocket::connection_ptr con;

    EventLoop::EventLoop& mEv;
    std::shared_ptr<spdlog::logger> mLogger;
  };

} // namespace FrogFish


#endif
