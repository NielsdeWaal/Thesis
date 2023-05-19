#ifndef __QUERY_MANAGER_HPP
#define __QUERY_MANAGER_HPP

#include "EventLoop.h"
#include "Query.hpp"
#include "QueryPostProcessingOps.hpp"
#include "Writer.hpp"

#include <variant>

template<std::size_t bufSize> class QueryManager: EventLoop::IEventLoopCallbackHandler {
public:
  QueryManager(EventLoop::EventLoop& ev, Writer<bufSize>& writer, MetaData& metadata)
  : mEv(ev)
  , mWriter(writer)
  , mMetadata(metadata) {
    mLogger = mEv.RegisterLogger("QueryManager");
    mEv.RegisterCallbackHandler(( EventLoop::IEventLoopCallbackHandler* ) this, EventLoop::EventLoop::LatencyType::Low);
    mQueries.resize(10);
  }

  // void CreateQuery(const std::string& query) {
  //   //  [\s,]*(~@|[\[\]{}()'`~^@]|"(?:\\.|[^\\"])*"?|;.*|[^\s\[\]{}('"`,;)]*)
  //   // std::regex parser(R"[\s,]*(~@|[\[\]{}()'`~^@]|\"(\?:\\.|[\^\\\"])*\"?|;.*|[^\s\[\]{}('\"`,;)]*)");
  //   std::regex parser("[\\s,]*(~@|[\\[\\]{}()'`~^@]|\"(?:\\\\.|[^\\\\\"])*\"?|;.*|[^\s\\[\\]{}('\"`,;)]*)");
  //   std::vector<std::string> atoms(std::sregex_token_iterator(query.begin(), query.end(), parser, -1), {});
  //   mLogger->info("Parsed query into: size: {}", atoms.size());
  //   for (const auto& str : atoms) {
  //     mLogger->warn("{}", str);
  //   }
  // }

  // void Parse(std::string_view buf);

  void SubmitQuery(std::string_view expression) {
    SeriesQuery::Expr query;
    std::uint64_t startTS{0}; // TODO Switch to optional to make 0 a possible value
    std::uint64_t endTS{0};
    std::uint64_t index{0};
    std::string_view metricName;
    std::string_view targetTag;
    std::vector<std::string> tagValues;
    auto token = NextToken(expression);
    bool settingIndex = false;
    bool settingRange = false;
    bool settingTags = false;
    bool settingMetric = false;
    bool settingGroup = false;
    GroupByOp group;
    std::vector<std::uint64_t> valStack;
    while (!(token.parsed.empty() && token.remaining.empty())) {
      using namespace SeriesQuery;
      mLogger->info("Token: {}", token.parsed);
      if (token.parsed == "->>") {
        mLogger->info("Generating new query");
      }

      if (token.parsed == "range") {
        settingRange = true;
      }
      if (token.parsed == "index") {
        settingIndex = true;
      }
      if (token.parsed == "tag") {
        settingTags = true;
      }
      if (token.parsed == "metric") {
        settingMetric = true;
      }
      if (token.parsed == "groupby") {
        settingGroup = true;
      }

      if (token.parsed == "and") {
        // mLogger->info("And token");
        add(query, AndExpr{});
      }
      if (token.parsed == "<") {
        add(query, LtExpr{});
      }
      if (token.parsed == ">") {
        add(query, GtExpr{});
      }

      if (token.parsed == "#TS") {
        add(query, TimestampLiteral{});
      }
      if (token.parsed == "#V") {
        add(query, ValueLiteral{});
      }

      if (token.parsed == "(" || token.parsed == ")") {
      }

      if (auto [isInt, val] = parse_int(token.parsed); isInt) {
        // mLogger->info("Int token: {}", val);
        if (settingRange) {
          if (startTS == 0) {
            startTS = val;
          } else {
            endTS = val;
            settingRange = false;
          }
        } else if (settingIndex) {
          index = val;
          settingIndex = false;
        } else {
          add(query, UnsignedLiteralExpr{val});
        }
      }

      if (token.parsed.starts_with("\"") && token.parsed.ends_with("\"")) {
        if (settingTags) {
          if (targetTag.empty()) {
            targetTag = StripQuotes(token.parsed);
          } else {
            tagValues.push_back(std::string{StripQuotes(token.parsed)});
          }
        } else if (settingMetric) {
          metricName = StripQuotes(token.parsed);
          settingMetric = false;
        }
      }

      if (settingGroup && token.parsed.ends_with("h")) {
        token.parsed.remove_suffix(1);
        if (auto [isInt, val] = parse_int(token.parsed); isInt) {
          group.GroupInterval = val * 3600000000000;
        }
      } else if (settingGroup) {
        if (token.parsed == "max") {
          group.type = GroupByOp::Type::MAX;
        } else if(token.parsed == "min") {
          group.type = GroupByOp::Type::MIN;
        } else if(token.parsed == "avg") {
          group.type = GroupByOp::Type::AVG;
        } else if(token.parsed == "count") {
          group.type = GroupByOp::Type::COUNT;
        }
      }

      if (settingGroup && group.GroupInterval != 0 && group.type != GroupByOp::Type::INVALID) {
        mLogger->info("Grouping data for every {} on op: {}", group.GroupInterval, static_cast<int>(group.type));
        settingGroup = false;
      }

      token = NextToken(token.remaining);
    }

    if (!targetTag.empty()) {
      mLogger->info("Matching {} against {}", targetTag, fmt::join(tagValues, ", "));
    }

    if (index == 0) {
      auto res = mMetadata.GetIndex(std::string{metricName}, {{std::string{targetTag}, tagValues}});
      if (res.has_value()) {
        index = res.value();
      } else {
        spdlog::error("No index found for {} and {}", metricName, targetTag);
      }
    }

    if (startTS == 0 && endTS == 0) {
      auto* root = mWriter.GetTreeForIndex(index).GetRoot();
      startTS = root->GetNodeStart();
      endTS = root->GetNodeEnd();
    }

    auto nodes = mWriter.GetTreeForIndex(index).Query(startTS, endTS);
    assert(nodes.has_value());

    std::vector<std::uint64_t> addrs;
    addrs.reserve(nodes->size());
    for (const TimeRange_t& tr : *nodes) {
      addrs.push_back(tr.ptr);
    }

    mLogger->info(
        "New query: \n\t range: {} - {}\n\t metric: {}\n\t index: {}\n\t nubmer of reads: {}\n\t counter: {}",
        startTS,
        endTS,
        metricName,
        index,
        addrs.size(),
        mQueryCounter);
    // QueryIO io{mEv, mWriter.GetNodeFileFd(), addrs, bufSize};
    // mQueries.emplace_back(, query);
    mQueries.emplace_back(mEv, mWriter.GetNodeFileFd(), addrs, bufSize, query, startTS, endTS, group, mQueryCounter);
    ++mQueryCounter;
  }

  void OnEventLoopCallback() override {
    // for (QueryIO& op : mQueries) {
    for (Query& op : mQueries) {
      if (!op.handled && op.IOBatch) {
        auto& res = op.IOBatch.GetResult();

        mLogger->info("Query finished for {} blocks", res.size());

        std::vector<EventLoop::DmaBuffer> resultBuffers;
        std::for_each(res.begin(), res.end(), [&resultBuffers](IOOP& resOp) {
          resultBuffers.emplace_back(std::move(resOp.buf));
        });

        std::vector<GroupOps> postGroups;
        std::uint64_t binTSOffset = op.startTS + op.postProcessing.GroupInterval;
        std::uint64_t binCounter = 0;
        if (op.postProcessing.type != GroupByOp::Type::INVALID) {
          mLogger->warn(
              "Group by detected, binning into {} groups of {} hour/ {} nanoseconds",
              (op.endTS - op.startTS) / op.postProcessing.GroupInterval,
              op.postProcessing.GroupInterval * 3600000000000,
              op.postProcessing.GroupInterval);
          postGroups.resize(((op.endTS - op.startTS) / op.postProcessing.GroupInterval) + 1);
          for (auto& group : postGroups) {
            switch(op.postProcessing.type) {
              case GroupByOp::Type::MAX: {
                group = MaxOp();
                break;
              }
              case GroupByOp::Type::MIN: {
                group = MinOp();
                break;
              }
              case GroupByOp::Type::AVG: {
                group = AvgOp();
                break;
              }
              case GroupByOp::Type::COUNT: {
                group = CountOp();
                break;
              }
              case GroupByOp::Type::INVALID: {
                mLogger->critical("Cannot create group op from INVALID type");
                assert(false);
              }
            }
          }
        }

        using namespace SeriesQuery;
        for (EventLoop::DmaBuffer& buf : resultBuffers) {
          DataPoint* points = ( DataPoint* ) buf.GetPtr();
          for (std::size_t i = 0; i < memtableSize; ++i) {
            if (points[i].timestamp >= op.startTS && points[i].timestamp <= op.endTS) {
              if (!std::holds_alternative<std::monostate>(op.QueryExpression)
                  && !evaluate(op.QueryExpression, points[i].timestamp, points[i].value)) {
                continue;
              }
              if (op.postProcessing.type != GroupByOp::Type::INVALID) {
                if (points[i].timestamp > binTSOffset) {
                  ++binCounter;
                  binTSOffset += op.postProcessing.GroupInterval;
                  mLogger->info("Switching to bin {}", binCounter);
                }
                // mLogger->info("Assigning to group: {}", binCounter);
                // postGroups.at(binCounter).Add(points[i].value);
                std::visit(
                    overloaded{
                        [&](MaxOp& arg) { arg.Add(points[i].value); },
                        [&](MinOp& arg) { arg.Add(points[i].value); },
                        [&](AvgOp& arg) { arg.Add(points[i].value); },
                        [&](CountOp& arg) { arg.Add(points[i].value); },
                        []([[maybe_unused]] std::monostate) { assert(false); },
                    },
                    postGroups.at(binCounter));
              } else {
                mLogger->info("res: {} -> {}", points[i].timestamp, points[i].value);
              }
            }
          }
        }

        if (postGroups.size() > 0) {
          for (auto& val : postGroups) {
            // mLogger->info("Bucket val: {}", val.GetVal());
            std::uint64_t opRes = std::visit(
                overloaded{
                    [&](MaxOp& arg) { return arg.GetVal(); },
                    [&](MinOp& arg) { return arg.GetVal(); },
                    [&](AvgOp& arg) { return arg.GetVal(); },
                    [&](CountOp& arg) { return arg.GetVal(); },
                    []([[maybe_unused]] std::monostate) -> std::uint64_t { assert(false); return 0;},
                },
                val);
            mLogger->info("Bucket val: {}", opRes);
          }
        }

        mLogger->info("Query done");
        op.handled = true;
        // mQueryManager.ParseQuery("(and (< #TS 1451621760000000000) (> #V 95))");

        // std::erase_if(mQueries, [&](const Query& q){return q.id == op.id;});
      }
    }

    // NOTE an erase call would cause other queries to be moved, invalidating pointers stored in eventloop
    while (mQueries.front().handled) {
      mLogger->info("Removing old query");
      mQueries.pop_front();
    }
  }

private:
  struct DataPoint {
    std::uint64_t timestamp;
    std::int64_t value;
  };

  struct GroupByOp {
    enum class Type : std::uint8_t {
      INVALID = 0,
      MAX,
      MIN,
      AVG,
      COUNT,
    };

    Type type{Type::INVALID};
    std::uint64_t GroupInterval{0};
  };
  // TODO attach metadata to measure latency
  // TODO Support subqueries for joining operations
  struct Query {
    Query(
        EventLoop::EventLoop& ev,
        int fd,
        const std::vector<std::uint64_t>& addrs,
        std::size_t blockSize,
        SeriesQuery::Expr expression,
        std::uint64_t start,
        std::uint64_t end,
        GroupByOp op,
        std::uint64_t id)
    : IOBatch(ev, fd, addrs, blockSize)
    , QueryExpression(expression)
    , startTS(start)
    , endTS(end)
    , postProcessing(op)
    , id(id) {}
    Query(): IOBatch(), QueryExpression(), id(0) {}
    QueryIO IOBatch;
    SeriesQuery::Expr QueryExpression;

    std::uint64_t startTS{0};
    std::uint64_t endTS{0};

    GroupByOp postProcessing;

    // Used together with query counter to allow queries to be deleted from queue
    std::uint64_t id;
    bool handled{false};
  };

  struct Token {
    std::string_view parsed;
    std::string_view remaining;
  };

  [[nodiscard]] constexpr Token NextToken(std::string_view input) {
    constexpr auto is_eol = [](auto character) { return character == '\n' || character == '\r'; };
    constexpr auto is_whitespace = [is_eol](auto character) {
      return character == ' ' || character == '\t' || is_eol(character);
    };

    constexpr auto consume = [=](auto ws_input, auto predicate) {
      auto begin = ws_input.begin();
      while (begin != ws_input.end() && predicate(*begin)) {
        ++begin;
      }
      return std::string_view{begin, ws_input.end()};
    };

    constexpr auto make_token = [=](auto token_input, std::size_t size) {
      return Token{token_input.substr(0, size), consume(token_input.substr(size), is_whitespace)};
    };

    input = consume(input, is_whitespace);

    // comment
    if (input.starts_with(';')) {
      input = consume(input, [=](char character) { return not is_eol(character); });
      input = consume(input, is_whitespace);
    }

    // list
    if (input.starts_with('(') || input.starts_with(')')) {
      return make_token(input, 1);
    }

    // literal list
    if (input.starts_with("'(")) {
      return make_token(input, 2);
    }

    // quoted string
    if (input.starts_with('"')) {
      bool in_escape = false;
      auto location = std::next(input.begin());
      while (location != input.end()) {
        if (*location == '\\') {
          in_escape = true;
        } else if (*location == '"' && !in_escape) {
          ++location;
          break;
        } else {
          in_escape = false;
        }
        ++location;
      }

      return make_token(input, static_cast<std::size_t>(std::distance(input.begin(), location)));
    }

    // everything else
    const auto value = consume(input, [=](char character) {
      return !is_whitespace(character) && character != ')' && character != '(';
    });

    return make_token(input, static_cast<std::size_t>(std::distance(input.begin(), value.begin())));
  }

  [[nodiscard]] std::pair<bool, std::uint64_t> parse_int(std::string_view input) {
    std::uint64_t parsed = 0;
    auto result = std::from_chars(input.data(), input.data() + input.size(), parsed);
    if (result.ec == std::errc() && result.ptr == input.data() + input.size()) {
      return {true, parsed};
    } else {
      return {false, parsed};
    }
  }

  std::string_view StripQuotes(std::string_view buf) {
    assert(buf.starts_with("\"") && buf.ends_with("\""));
    buf.remove_suffix(1);
    buf.remove_prefix(1);
    return buf;
  }

  EventLoop::EventLoop& mEv;

  std::deque<Query> mQueries;

  static constexpr std::size_t memtableSize = bufSize / sizeof(DataPoint);
  MetaData& mMetadata;
  Writer<bufSize>& mWriter;

  std::uint64_t mQueryCounter{0};

  std::shared_ptr<spdlog::logger> mLogger;
};

#endif