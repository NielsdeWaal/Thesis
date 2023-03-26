#ifndef __INFLUX_PARSER
#define __INFLUX_PARSER

#include <cstring>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <optional>
#include <span>
#include <spdlog/logger.h>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

struct InfluxMeasurement {
  std::string name;
  std::variant<std::uint64_t, float> value;
};

struct InfluxMessage {
  std::string name;
  std::uint64_t index;
  // std::vector<std::string> tags;
  // std::vector<std::string> values;
  std::vector<InfluxMeasurement> measurments;
  std::uint64_t timestamp;
};

class InfluxParser {
public:
  // InfluxParser() = default;
  InfluxParser(const std::string& filename)
  : mFile{filename} {}

  // TODO remove/replace with 'next' for when parser is opened
  // in file mode
  void Parse(std::string& line, InfluxMessage& result) {
    std::string::size_type pos = 0;
    std::string token;

    // Extract the measurement
    pos = line.find(' ');
    if (pos != std::string::npos) {
      result.name = line.substr(0, pos);
      line.erase(0, pos + 1);
    }

    pos = 0;
    // Extract the tags
    while (pos != std::string::npos) {
      pos = line.find('=');
      token = line.substr(0, pos);
      std::string name{token};
      // result.tags.push_back(token);
      line.erase(0, pos + 1);
      pos = line.find(',');
      token = line.substr(0, pos);
      // If this is the last value, escape
      if (auto it = token.find(" "); it != std::string::npos) {
        token = line.substr(0, it);
        // result.values.push_back(token);
        result.measurments.push_back({std::move(name), GetValue(token)});
        line.erase(0, it + 1);
        break;
      }
      // result.values.push_back(token);
      result.measurments.push_back({std::move(name), GetValue(token)});
      line.erase(0, pos + 1);
    }

    // Extract timestamp
    pos = line.find("\n");
    token = line.substr(0, pos);
    std::uint64_t ts = std::stoll(token);
    result.timestamp = ts;
  }

  void ParseAll(std::vector<InfluxMessage>& messages) {
    for (std::string line; std::getline(mFile, line);) {
      messages.emplace_back();
      Parse(line, messages.back());
    }
  }

  // TODO write iterator for file mode

private:
  std::variant<std::uint64_t, float> GetValue(const std::string& encoded) {
    if (encoded.ends_with("i")) {
      return static_cast<std::uint64_t>(std::stoll(encoded.substr(0, encoded.size() - 1)));
    }
  }

  std::ifstream mFile;
};

class InputManager {
public: 
  InputManager() = default;
  InputManager(EventLoop::EventLoop& ev, const std::string& filename)
  : mParser(filename) 
  {
    mLogger = ev.RegisterLogger("InputManager");
    mMessages.reserve(2000000);
    mParser.ParseAll(mMessages);

    for (InfluxMessage& msg : mMessages) {
      for (InfluxMeasurement& measurement : msg.measurments) {
        std::string name{msg.name + "." + measurement.name};
        if (!mIndex.contains(name)) {
          mIndex[name] = mIndexCounter;
          mLogger->info("New series for {}, assigning id: {}", name, mIndexCounter);
          // mTrees[mIndexCounter] = MetricTree{};
          ++mIndexCounter;
        }
      }
    }
  }

  std::optional<std::span<InfluxMessage>> ReadChunk(std::size_t length) {
    if(mReaderOffset >= mMessages.size()) {
      return std::nullopt;
    }

    // auto maxOffset = std::min(mReaderOffset + length, mMessages.size());
    // auto res = std::span(mMessages.begin() + mReaderOffset, mMessages.begin() + mReaderOffset + length);
    auto res = std::span(mMessages.begin() + mReaderOffset, mReaderOffset + length <= mMessages.size() ? length : 0U);
    if(res.empty()) {
      return std::nullopt;
    }
    mReaderOffset += length;
    return res;
  }

  auto begin() {
    return mMessages.begin();
  }

  auto end() {
    return mMessages.end();
  }

  // std::unordered_map<std::string, std::uint64_t>& GetMapping() {
    
  // }

private: 
  InfluxParser mParser;
  std::vector<InfluxMessage> mMessages;
  std::unordered_map<std::string, std::uint64_t> mIndex{};
  std::uint64_t mIndexCounter{0};
  std::size_t mReaderOffset{0};

  std::shared_ptr<spdlog::logger> mLogger;
};

template<> struct fmt::formatter<InfluxMeasurement> {
  template<typename ParseContext> constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  // Formats the point p using the parsed format specification (presentation)
  // stored in this formatter.
  template<typename FormatContext>
  auto format(const InfluxMeasurement& p, FormatContext& ctx) const -> decltype(ctx.out()) {
    if (std::holds_alternative<std::uint64_t>(p.value)) {
      return fmt::format_to(ctx.out(), "{} = {}", p.name, std::get<std::uint64_t>(p.value));
    } else {
      return fmt::format_to(ctx.out(), "{} = {}", p.name, std::get<float>(p.value));
    }
  }
};

#endif // __INFLUX_PARSER
