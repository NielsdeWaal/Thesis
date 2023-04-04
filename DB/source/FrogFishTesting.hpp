#ifndef __FROGFISH_TESTING
#define __FROGFISH_TESTING

#include "Query.hpp"

#include <iostream>
#include <string>
#include <variant>

struct LoadingData {
  std::string filename; // states which file to load
};

struct Ingesting {};

struct PrepareQuery {};

// TODO query is very rigid right now, having to accept the ts and value
struct Querying {
  std::string target; // target metric to query
  std::function<bool(SeriesQuery::UnsignedLiteralExpr, SeriesQuery::SignedLiteralExpr)>
      queryExpression; // query filter expression
};

// TODO maybe hold information for things like the hdrhistogram
struct Done {};

class TestingHelper: public std::variant<LoadingData, Ingesting, PrepareQuery, Querying, Done> {
public:
  using variant::variant;

  bool IsLoadingData() const {
    return std::holds_alternative<LoadingData>(*this);
  }

  bool IsIngesting() const {
    return std::holds_alternative<Ingesting>(*this);
  }

  bool IsPreparingQuery() const {
    return std::holds_alternative<PrepareQuery>(*this);
  }

  bool IsQuerying() const {
    return std::holds_alternative<Querying>(*this);
  }

  bool IsDone() const {
    return std::holds_alternative<Done>(*this);
  }

  [[nodiscard]] std::uint64_t SetLoadingData(const std::string& filename) {
    auto transition = Common::MONOTONIC_CLOCK::Now() - mTransitionLatency;
    *this = LoadingData{.filename = filename};
    return Common::MONOTONIC_CLOCK::ToNanos(transition);
  }

  [[nodiscard]] std::uint64_t SetIngesting() {
    auto transition = Common::MONOTONIC_CLOCK::Now() - mTransitionLatency;
    *this = Ingesting{};
    return Common::MONOTONIC_CLOCK::ToNanos(transition);
  }

  [[nodiscard]] std::uint64_t SetPrepareQuery() {
    auto ingest = Common::MONOTONIC_CLOCK::Now() - mTransitionLatency;
    *this = PrepareQuery{};
    return Common::MONOTONIC_CLOCK::ToNanos(ingest);
  }

  void SetQuerying(
      const std::string& target,
      const std::function<bool(SeriesQuery::UnsignedLiteralExpr, SeriesQuery::SignedLiteralExpr)>& filterExpression) {
    *this = Querying{.target = target, .queryExpression = filterExpression};
    (*this).mIngestionLatency = mIngestionLatency;
    mQueryLatency = Common::MONOTONIC_CLOCK::Now();
  }

  std::string& GetFilename() {
    return std::get<LoadingData>(*this).filename;
  }

  bool ExecFilter(SeriesQuery::UnsignedLiteralExpr ts, SeriesQuery::SignedLiteralExpr val) {
    return std::get<Querying>(*this).queryExpression(ts, val);
  }

  [[nodiscard]] std::uint64_t Finalize() {
    auto query = Common::MONOTONIC_CLOCK::Now() - mTransitionLatency;
    *this = Done{};
    return Common::MONOTONIC_CLOCK::ToNanos(query);
  }

  std::uint64_t GetIngestionLatency() const {
    return mIngestionLatency;
  }

private:
  Common::MONOTONIC_TIME mIngestionLatency{0};
  Common::MONOTONIC_TIME mQueryLatency{0};
  Common::MONOTONIC_TIME mTransitionLatency{Common::MONOTONIC_CLOCK::Now()};
};

#endif // __FROGFISH_TESTING
