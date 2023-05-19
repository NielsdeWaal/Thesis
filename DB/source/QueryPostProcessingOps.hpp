#ifndef __POST_PROCESSING_OPS
#define __POST_PROCESSING_OPS

#include <algorithm>
#include <cstdint>
#include <variant>

class GroupOp {
public:
  virtual ~GroupOp() = default;
  virtual void Add(std::uint64_t) = 0;
  virtual std::uint64_t GetVal() const = 0;
};

class MaxOp  {
public:
  MaxOp() = default;
  MaxOp(std::uint64_t initial): val(initial) {}

  MaxOp& operator+=(const MaxOp& rhs) {
    // val += rhs.val;
    val = std::max(val, rhs.val);
    return *this;
  }

  void Add(std::uint64_t newVal) {
    val = std::max(newVal, val);
  }

  std::uint64_t GetVal() const {
    return val;
  }

private:
  std::uint64_t val{0};
};

class MinOp {
public:
  MinOp() = default;
  MinOp(std::uint64_t initial): val(initial) {}

  MinOp& operator+=(const MinOp& rhs) {
    // val += rhs.val;
    val = std::min(val, rhs.val);
    return *this;
  }

  void Add(std::uint64_t newVal) {
    val = std::min(newVal, val);
  }

  std::uint64_t GetVal() const {
    return val;
  }

private:
  std::uint64_t val{0};
};

class AvgOp {
public:
  AvgOp() = default;
  AvgOp(std::uint64_t initial): val(initial) {}

  void Add(std::uint64_t newVal) {
    val = (newVal + count * val) / count + 1;
    ++count;
  }

  std::uint64_t GetVal() const {
    return val;
  }

private:
  std::uint64_t val{0};
  std::size_t count{0};
};

class CountOp {
public:
  CountOp() = default;
  // CountOp(std::uint64_t initial): val(initial) {}

  void Add([[maybe_unused]] std::uint64_t) {
    ++val;
  }

  std::uint64_t GetVal() const {
    return val;
  }

private:
  std::uint64_t val{0};
};

using GroupOps = std::variant<std::monostate, MaxOp, MinOp, AvgOp, CountOp>;

#endif
