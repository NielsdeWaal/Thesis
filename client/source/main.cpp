#include "EventLoop.h"
#include "FrogFishClient.hpp"

class Handler {
public:
  Handler(EventLoop::EventLoop& ev) 
  : mEv(ev)
  , mClient(mEv)
  {
    mLogger = mEv.RegisterLogger("Handler");
  }

  void Configure() {
    auto config = mEv.GetConfigTable("Client");
    // mIngestionMethod = config->get_as<int>("IngestionMethod").value_or(-1);
    mLogger->info(
        "Configured:\n\t testing file: {}\n\t Using management port: {}",
        config->get_as<std::string>("TestingFile").value_or(""),
        config->get_as<std::uint16_t>("ManagementPort").value_or(0));
    // [[maybe_unused]] auto _ = mTestingHelper.SetLoadingData(config->get_as<std::string>("file").value_or(""));
    // mIngestPort.Configure(config->get_as<std::uint16_t>("IngestBasePort").value_or(0));
    mClient.Configure();
  }

private:  
  EventLoop::EventLoop& mEv;
  FrogFishClient mClient;
  std::shared_ptr<spdlog::logger> mLogger;
};

int main() {
  EventLoop::EventLoop loop;
  loop.LoadConfig("FrogFishClient.toml");
  loop.Configure();

  Handler app(loop);
  app.Configure();

  loop.Run();
}
