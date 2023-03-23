#ifndef __INFLUX_PARSER
#define __INFLUX_PARSER

#include <cstring>
#include <string>
#include <variant>
#include <vector>

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

struct InfluxMeasurement {
  std::string name;
  std::variant<std::uint64_t, float> value;
};

struct InfluxMessage {
  std::string name;
  // std::vector<std::string> tags;
  // std::vector<std::string> values;
  std::vector<InfluxMeasurement> measurments;
  std::uint64_t timestamp;
};

class InfluxParser {
public:
  InfluxParser() = default;
  InfluxParser(const std::string &filename) {}

  // TODO remove/replace with 'next' for when parser is opened
  // in file mode
  void Parse(std::string &line, InfluxMessage& result) {
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

  // TODO write iterator for file mode

private:
  std::variant<std::uint64_t, float> GetValue(const std::string& encoded) {
    if(encoded.ends_with("i")) {
      return static_cast<std::uint64_t>(std::stoll(encoded.substr(0, encoded.size() - 1)));
    }
  }
};

template <> struct fmt::formatter<InfluxMeasurement> {
  // Presentation format: 'f' - fixed, 'e' - exponential.
  // char presentation = 'f';

  template<typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    return ctx.begin();
  }

  // Formats the point p using the parsed format specification (presentation)
  // stored in this formatter.
  template <typename FormatContext>
  auto format(const InfluxMeasurement& p, FormatContext& ctx) const -> decltype(ctx.out()) {
    // ctx.out() is an output iterator to write to.
    // return presentation == 'f'
    //           ? fmt::format_to(ctx.out(), "({:.1f}, {:.1f})", p.x, p.y)
    //           : fmt::format_to(ctx.out(), "({:.1e}, {:.1e})", p.x, p.y);
    // return fmt::format_to(ctx.out(), "{} = {}", p.name, p.value);
    if(std::holds_alternative<std::uint64_t>(p.value)) {
      return fmt::format_to(ctx.out(), "{} = {}", p.name, std::get<std::uint64_t>(p.value));
    } else {
      return fmt::format_to(ctx.out(), "{} = {}", p.name, std::get<float>(p.value));
    }
  }
};

#endif // __INFLUX_PARSER
