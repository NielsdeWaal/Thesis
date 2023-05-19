#include "FrogFish.hpp"

int main() {
  EventLoop::EventLoop loop;
  loop.LoadConfig("FrogFish.toml");
  loop.Configure();

  Database app(loop);
  app.Configure();

  loop.Run();

  return 0;
}
