#ifndef __INFLUX_PARSER
#define __INFLUX_PARSER

#include "IngestionProtocol/proto.capnp.h"
#include "lexyparser.hpp"
#include "SeriesIndex.hpp"

#include <arrow/array.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <capnp/serialize.h>
#include <cstring>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <lexy/action/parse.hpp>
#include <lexy/input/file.hpp>
#include <lexy/input/string_input.hpp>
#include <lexy_ext/report_error.hpp>
#include <optional>
#include <span>
#include <spdlog/logger.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
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

// TODO can be removed
class InfluxParser {
public:
  // InfluxParser() = default;
  InfluxParser(const std::string& filename): mFile{filename} {}

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
  InputManager(EventLoop::EventLoop& ev): /*mParser(filename),*/ mIndex(ev) {
    mLogger = ev.RegisterLogger("InputManager");
    mSeriesName.reserve(255);
  }

  EventLoop::uio::task<> ReadFromFile(const std::string& filename) {
    co_await mIndex.SetupFiles();

    auto file = lexy::read_file<lexy::utf8_encoding>(filename.c_str());
    auto result = lexy::parse<grammar::InfluxMessage>(file.buffer(), lexy_ext::report_error.path(filename.c_str()));
    if (!result.has_value()) {
      mLogger->critical("Failed to parse file");
      throw std::runtime_error("Failed to parse file");
    }
    mMessages = result.value();
    for (IMessage& msg : mMessages) {
      for (InfluxKV& kv : msg.measurements) {
        auto& val = std::get<InfluxMValue>(kv.value);
        if (auto index = mIndex.GetIndex(msg.name); index.has_value()) {
          kv.index = index.value();
        } else {
          // mLogger->info("New series for {}, assigning id: {}", name, mIndexCounter);
          kv.index = co_await mIndex.AddSeries(msg.name);
        }
      }
    }
  }

  EventLoop::uio::task<> ReadFromArrowFile(const std::string& filename) {
    co_await mIndex.SetupFiles();
    mLogger->info("Reading {}", filename);
    {
      auto res = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
      assert(res.ok());
      infile = res.ValueOrDie();
    }
    {
      auto res = arrow::ipc::RecordBatchFileReader::Open(infile);
      assert(res.ok());
      mFileReader = res.ValueOrDie();
      auto schema = mFileReader->schema();
    }
    mNumRowGroups = mFileReader->num_record_batches();
  }

  EventLoop::uio::task<> ReadFromCapnpFile(const std::string& filename) {
    co_await mIndex.SetupFiles();

    int fd = open(filename.c_str(), O_RDONLY);
    mCapWrapper = std::make_unique<CapnpWrapper>(fd);
  }
  std::optional<kj::ArrayPtr<capnp::word>> GetCapReader() {
    auto words = mCapWrapper->words;
    return words;
  }

  void PushCapOffset(capnp::word* end) {
    mCapWrapper->words = kj::arrayPtr(end, mCapWrapper->words.end());
  }

  EventLoop::uio::task<std::optional<std::span<IMessage>>> ReadArrowChunk() {
    if (mRowGroupIndex == mNumRowGroups) {
      mLogger->warn("Reached end of input file");
      co_return std::nullopt;
    }
    mMessages.clear();
    std::shared_ptr<arrow::RecordBatch> batch;
    // ARROW_ASSIGN_OR_RAISE(batch, mFileReader->ReadRecordBatch(mReaderOffset));
    {
      auto res = mFileReader->ReadRecordBatch(mReaderOffset);
      if (res.ok()) {
        batch = res.ValueOrDie();
      } else {
        mLogger->critical("Failed to read arrow chunk: {}", res.status().ToString());
        exit(1);
        // throw std::runtime_error(res.status().ToString());
      }
    }

    auto str_col = batch->column(0);
    const auto str_arr = std::static_pointer_cast<arrow::StringArray>(str_col);
    auto data = str_arr->value_data();

    auto input = lexy::string_input<lexy::utf8_encoding>(( const char* ) data->data(), data->size());
    auto result = lexy::parse<grammar::InfluxMessage>(input, lexy_ext::report_error);
    if (!result.has_value()) {
      mLogger->critical("Failed to parse chunk");
      throw std::runtime_error("Failed to parse chunk");
    }

    mRowGroupIndex += 1;
    mMessages = result.value();
    for (IMessage& msg : mMessages) {
      for (InfluxKV& kv : msg.measurements) {
        auto index = mIndex.GetIndex(msg.name + kv.name);
        if (!index.has_value()) {
          mLogger->warn("inserting {}", msg.name + kv.name);
          kv.index = mIndex.InsertSeries(msg.name + kv.name);
          // co_await mIndex.AddSeries(mSeriesName);
        } else {
          kv.index = index.value();
        }
        mSeriesName.clear();
      }
    }

    co_return std::span(mMessages.begin(), mMessages.end());
  }

  // std::optional<std::span<InfluxMessage>> ReadChunk(std::size_t length) {
  std::optional<std::span<IMessage>> ReadChunk(std::size_t length) {
    if (mReaderOffset >= mMessages.size()) {
      return std::nullopt;
    }

    // auto maxOffset = std::min(mReaderOffset + length, mMessages.size());
    // auto res = std::span(mMessages.begin() + mReaderOffset, mMessages.begin() + mReaderOffset + length);
    auto res = std::span(mMessages.begin() + mReaderOffset, mReaderOffset + length <= mMessages.size() ? length : 0U);
    if (res.empty()) {
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

  auto GetIndex(const std::string& name) {
    return mIndex.GetIndex(name);
  }

  std::uint64_t InsertSeries(const std::string& name) {
    // int index = co_await mIndex.InsertSeries(name);
    // co_return index;
    return mIndex.InsertSeries(name);
  }

  // std::unordered_map<std::string, std::uint64_t>& GetMapping() {

  // }

private:
  struct CapnpWrapper {
    CapnpWrapper(int fd) {
      struct stat stats;
      ::fstat(fd, &stats);
      std::size_t size = stats.st_size;
      void* data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
      if(data == MAP_FAILED) {
        spdlog::critical("Failed to mmap");
      }
      ::madvise(data, size, MADV_SEQUENTIAL);

      words = kj::ArrayPtr<capnp::word>(reinterpret_cast<capnp::word*>(data), size / sizeof(capnp::word));
    }

    kj::ArrayPtr<capnp::word> words;
  };
  std::vector<IMessage> mMessages;

  std::uint64_t mIndexCounter{0};
  std::size_t mReaderOffset{0};

  // Arrow related parameters
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> mFileReader;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  int mNumRowGroups{0};
  int mRowGroupIndex{0};

  // capnp related parameters
  std::unique_ptr<CapnpWrapper> mCapWrapper;

  std::string mSeriesName{};

  Index mIndex;

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
