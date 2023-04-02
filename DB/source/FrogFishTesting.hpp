#ifndef __FROGFISH_TESTING
#define __FROGFISH_TESTING

#include "Query.hpp"

#include <string>
#include <variant>

struct Ingesting {
  std::string filename; // states which file to load
};

// TODO query is very rigid right now, having to accept the ts and value
struct Quering {
  std::string target; // target metric to query
  std::function<bool(std::uint64_t, std::uint64_t)> queryExpression; // query filter expression
};

// TODO maybe hold information for things like the hdrhistogram
struct Done {};

class TestingHelper: public std::variant<Ingesting, Quering, Done> {
public:
  using variant::variant;

  bool IsIngesting() const {
    return std::holds_alternative<Ingesting>(*this);
  }

  bool IsQuering() const {
    return std::holds_alternative<Quering>(*this);
  }

  bool IsDone() const {
    return std::holds_alternative<Done>(*this);
  }

  void SetIngesting(const std::string& filename) {
    *this = Ingesting{.filename = filename};
  }

  void SetQuerying(const std::string& target, const std::function<bool(std::uint64_t, std::uint64_t)>& filterExpression) {
    *this = Quering{.target = target, .queryExpression = filterExpression};
  }

  std::string& GetFilename() {
    return std::get<Ingesting>(*this).filename;
  }

  bool ExecFilter(std::uint64_t ts, std::uint64_t val) {
    return std::get<Quering>(*this).queryExpression(ts, val);
  }

  void Finalize() const {}
};

#endif // __FROGFISH_TESTING
