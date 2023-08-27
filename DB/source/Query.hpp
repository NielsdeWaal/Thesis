#ifndef __QUERY
#define __QUERY

#include "DmaBuffer.h"
#include "EventLoop.h"
#include "UringCommands.h"

#include <charconv>
#include <cstdint>
#include <memory>
#include <regex>
#include <spdlog/spdlog.h>
#include <variant>
#include <vector>

// TODO there is an opportunity to do coalessing on the reads here
// When multiple sequential reads are detected, these can be merged into larger reads
// for better efficiency. Might need some kind of 'max' setting dictation the maximum
// size of such a read.

struct IOOP {
  IOOP() = default;
  IOOP(int pos, int len, EventLoop::DmaBuffer buf): pos(pos), len(len), buf(std::move(buf)) {}

  IOOP(const IOOP&) = default;
  IOOP(IOOP&&) = default;
  IOOP& operator=(const IOOP&) = default;
  IOOP& operator=(IOOP&&) = default;

  std::uint64_t pos{0};
  std::size_t len{0};
  EventLoop::DmaBuffer buf;
};

class QueryIO: EventLoop::IUringCallbackHandler {
public:
  QueryIO() = default;

  QueryIO(EventLoop::EventLoop& ev, int fd, std::vector<IOOP>& ops) {
    mInFlightIO.reserve(ops.size());
    for (IOOP& op : ops) {
      std::unique_ptr<EventLoop::UserData> data = std::make_unique<EventLoop::UserData>();

      data->mCallback = this;
      data->mType = EventLoop::SourceType::Read;
      data->mInfo = EventLoop::READ{.fd = fd, .buf = op.buf.GetPtr(), .len = op.len, .pos = op.pos};

      ev.QueueStandardRequest(std::move(data));

      // mInFlightIO.push_back(IOOP{.pos = op.pos, .len = op.len, .buf = std::move(op.buf)});
      mInFlightIO.emplace_back(op.pos, op.len, std::move(op.buf));
    }
    mIOCount = ops.size();
  }
  QueryIO(EventLoop::EventLoop& ev, int fd, const std::vector<std::uint64_t>& addrs, std::size_t blockSize) {
    mInFlightIO.reserve(addrs.size());
    for (const std::uint64_t addr : addrs) {
      std::unique_ptr<EventLoop::UserData> data = std::make_unique<EventLoop::UserData>();
      EventLoop::DmaBuffer buf = ev.AllocateDmaBuffer(blockSize);

      data->mCallback = this;
      data->mType = EventLoop::SourceType::Read;
      data->mInfo = EventLoop::READ{.fd = fd, .buf = buf.GetPtr(), .len = blockSize, .pos = addr};

      ev.QueueStandardRequest(std::move(data));

      // mInFlightIO.push_back(IOOP{.pos = addr, .len = blockSize, .buf = std::move(buf)});
      mInFlightIO.emplace_back(addr, blockSize, std::move(buf));
    }
    mIOCount = addrs.size();
  }

  operator bool() const {
    return mDone;
  }

  void OnCompletion(
      [[maybe_unused]] EventLoop::CompletionQueueEvent& cqe,
      [[maybe_unused]] const EventLoop::UserData* data) override {
    // const EventLoop::READ readOperation = static_cast<EventLoop::READ>(std::get<EventLoop::READ>(data->mInfo));
    // std::size_t removed = std::erase_if(mInFlightIO, [&readOperation](const IOOP& op) {
    //   return op.pos == readOperation.pos;
    // });
    --mIOCount;
    if (mIOCount == 0) {
      mDone = true;
    }
    // assert(removed == 1);
  }

  std::vector<IOOP>& GetResult() {
    return mInFlightIO;
  }

private:
  std::vector<IOOP> mInFlightIO{};
  std::size_t mIOCount{0};
  bool mDone{false};
};

class QueryBuilder {};
namespace SeriesQuery {
  template<class... Ts> struct overloaded: Ts... {
    using Ts::operator()...;
  };

  template<typename T> class box {
    // Wrapper over unique_ptr.
    std::unique_ptr<T> _impl;

  public:
    // Automatic construction from a `T`, not a `T*`.
    box(T&& obj): _impl(new T(std::move(obj))) {}
    box(const T& obj): _impl(new T(obj)) {}

    // Copy constructor copies `T`.
    box(const box& other): box(*other._impl) {}
    box& operator=(const box& other) {
      *_impl = *other._impl;
      return *this;
    }

    // box(const box&) = delete;
    box(box&&) = default;

    // unique_ptr destroys `T` for us.
    ~box() = default;

    // Access propagates constness.
    T& operator*() {
      return *_impl;
    }
    const T& operator*() const {
      return *_impl;
    }

    T* operator->() {
      return _impl.get();
    }
    const T* operator->() const {
      return _impl.get();
    }

    bool complete() const {
      return _impl.get()->complete();
    }
  };

  struct UnsignedLiteralExpr {
    UnsignedLiteralExpr(std::uint64_t val): value(val) {}
    std::uint64_t value;
    bool complete() const {
      return true;
    }
  };

  struct SignedLiteralExpr {
    SignedLiteralExpr(std::int64_t val): value(val) {}
    std::int64_t value;
    bool complete() const {
      return true;
    }
  };

  struct TimestampLiteral {
    TimestampLiteral() = default;
    bool complete() const {
      return true;
    }
    // TimestampLiteral(std::uint64_t val): value(val) {}
    // std::uint64_t value;
  };

  struct ValueLiteral {
    ValueLiteral() = default;
    bool complete() const {
      return true;
    }
    // ValueLiteral(std::int64_t val): value(val) {}
    // std::int64_t value;
  };

  using Expr = std::variant<
      std::monostate,
      UnsignedLiteralExpr,
      SignedLiteralExpr,
      TimestampLiteral,
      ValueLiteral,
      box<struct EqExpr>,
      box<struct OrExpr>,
      box<struct AndExpr>,
      box<struct GtExpr>,
      box<struct LtExpr>,
      box<struct AddExpr>>;

  struct EqExpr {
    Expr lhs, rhs;
    bool complete() const {
      return !std::holds_alternative<std::monostate>(lhs) && !std::holds_alternative<std::monostate>(rhs);
    }
  };

  struct OrExpr {
    Expr lhs, rhs;
    bool complete() const {
      return !std::holds_alternative<std::monostate>(lhs) && !std::holds_alternative<std::monostate>(rhs);
    }
  };

  struct AndExpr {
    Expr lhs, rhs;
    bool complete() const {
      return !std::holds_alternative<std::monostate>(lhs) && !std::holds_alternative<std::monostate>(rhs);
    }
  };

  struct GtExpr {
    Expr lhs, rhs;
    bool complete() const {
      return !std::holds_alternative<std::monostate>(lhs) && !std::holds_alternative<std::monostate>(rhs);
    }
  };

  struct LtExpr {
    Expr lhs, rhs;
    bool complete() const {
      return !std::holds_alternative<std::monostate>(lhs) && !std::holds_alternative<std::monostate>(rhs);
    }
  };

  struct AddExpr {
    Expr lhs, rhs;
    bool complete() const {
      return !std::holds_alternative<std::monostate>(lhs) && !std::holds_alternative<std::monostate>(rhs);
    }
  };

  void add(Expr& curr, Expr newExpr) {
    struct visitor {
      visitor(Expr& childExpression, Expr& parentExpression): expression(childExpression), parent(parentExpression) {}
      void operator()([[maybe_unused]] const UnsignedLiteralExpr& expr) {
        assert(false);
      }

      void operator()([[maybe_unused]] const SignedLiteralExpr& expr) {
        assert(false);
      }

      void operator()([[maybe_unused]] const TimestampLiteral& expr) {
        assert(false);
      }

      void operator()([[maybe_unused]] const ValueLiteral& expr) {
        assert(false);
      }

      void operator()(box<EqExpr>& expr) {
        // auto lhs = std::visit(*this, expr->lhs);
        // auto rhs = std::visit(*this, expr->rhs);
        if (std::holds_alternative<std::monostate>(expr->lhs)) {
          expr->lhs = expression;
        } else {
          bool lhsComplete = std::visit(
              overloaded{
                  [](auto arg) -> bool { return arg.complete(); },
                  [](std::monostate) -> bool {
                    assert(false);
                    return false;
                  }},
              expr->lhs);
          if (!lhsComplete) {
            std::visit(visitor{expression, expr->lhs}, expr->lhs);
          } else if (std::holds_alternative<std::monostate>(expr->rhs)) {
            expr->rhs = expression;
          } else {
            bool rhsComplete = std::visit(
                overloaded{
                    [](auto arg) -> bool { return arg.complete(); },
                    [](std::monostate) -> bool {
                      assert(false);
                      return false;
                    },
                },
                expr->rhs);
            if (!rhsComplete) {
              std::visit(visitor{expression, expr->rhs}, expr->rhs);
            }
          }
        }
      }

      void operator()(box<OrExpr>& expr) {
        if (std::holds_alternative<std::monostate>(expr->lhs)) {
          expr->lhs = expression;
        } else {
          bool lhsComplete = std::visit(
              overloaded{
                  [](auto arg) -> bool { return arg.complete(); },
                  [](std::monostate) -> bool {
                    assert(false);
                    return false;
                  }},
              expr->lhs);
          if (!lhsComplete) {
            std::visit(visitor{expression, expr->lhs}, expr->lhs);
          } else if (std::holds_alternative<std::monostate>(expr->rhs)) {
            expr->rhs = expression;
          } else {
            bool rhsComplete = std::visit(
                overloaded{
                    [](auto arg) -> bool { return arg.complete(); },
                    [](std::monostate) -> bool {
                      assert(false);
                      return false;
                    },
                },
                expr->rhs);
            if (!rhsComplete) {
              std::visit(visitor{expression, expr->rhs}, expr->rhs);
            }
          }
        }
      }

      void operator()(box<AndExpr>& expr) {
        if (std::holds_alternative<std::monostate>(expr->lhs)) {
          expr->lhs = expression;
        } else {
          bool lhsComplete = std::visit(
              overloaded{
                  [](auto arg) -> bool { return arg.complete(); },
                  [](std::monostate) -> bool {
                    assert(false);
                    return false;
                  }},
              expr->lhs);
          if (!lhsComplete) {
            std::visit(visitor{expression, expr->lhs}, expr->lhs);
          } else if (std::holds_alternative<std::monostate>(expr->rhs)) {
            expr->rhs = expression;
          } else {
            bool rhsComplete = std::visit(
                overloaded{
                    [](auto arg) -> bool { return arg.complete(); },
                    [](std::monostate) -> bool {
                      assert(false);
                      return false;
                    },
                },
                expr->rhs);
            if (!rhsComplete) {
              std::visit(visitor{expression, expr->rhs}, expr->rhs);
            }
          }
        }
      }

      void operator()(box<GtExpr>& expr) {
        if (std::holds_alternative<std::monostate>(expr->lhs)) {
          expr->lhs = expression;
        } else {
          bool lhsComplete = std::visit(
              overloaded{
                  [](auto arg) -> bool { return arg.complete(); },
                  [](std::monostate) -> bool {
                    assert(false);
                    return false;
                  }},
              expr->lhs);
          if (!lhsComplete) {
            std::visit(visitor{expression, expr->lhs}, expr->lhs);
          } else if (std::holds_alternative<std::monostate>(expr->rhs)) {
            expr->rhs = expression;
          } else {
            bool rhsComplete = std::visit(
                overloaded{
                    [](auto arg) -> bool { return arg.complete(); },
                    [](std::monostate) -> bool {
                      assert(false);
                      return false;
                    },
                },
                expr->rhs);
            if (!rhsComplete) {
              std::visit(visitor{expression, expr->rhs}, expr->rhs);
            }
          }
        }
      }
      void operator()(box<LtExpr>& expr) {
        if (std::holds_alternative<std::monostate>(expr->lhs)) {
          expr->lhs = expression;
        } else {
          bool lhsComplete = std::visit(
              overloaded{
                  [](auto arg) -> bool { return arg.complete(); },
                  [](std::monostate) -> bool {
                    assert(false);
                    return false;
                  }},
              expr->lhs);
          if (!lhsComplete) {
            std::visit(visitor{expression, expr->lhs}, expr->lhs);
          } else if (std::holds_alternative<std::monostate>(expr->rhs)) {
            expr->rhs = expression;
          } else {
            bool rhsComplete = std::visit(
                overloaded{
                    [](auto arg) -> bool { return arg.complete(); },
                    [](std::monostate) -> bool {
                      assert(false);
                      return false;
                    },
                },
                expr->rhs);
            if (!rhsComplete) {
              std::visit(visitor{expression, expr->rhs}, expr->rhs);
            }
          }
        }
      }
      void operator()(box<AddExpr>& expr) {
        if (std::holds_alternative<std::monostate>(expr->lhs)) {
          expr->lhs = expression;
        } else {
          bool lhsComplete = std::visit(
              overloaded{
                  [](auto arg) -> bool { return arg.complete(); },
                  [](std::monostate) -> bool {
                    assert(false);
                    return false;
                  }},
              expr->lhs);
          if (!lhsComplete) {
            std::visit(visitor{expression, expr->lhs}, expr->lhs);
          } else if (std::holds_alternative<std::monostate>(expr->rhs)) {
            expr->rhs = expression;
          } else {
            bool rhsComplete = std::visit(
                overloaded{
                    [](auto arg) -> bool { return arg.complete(); },
                    [](std::monostate) -> bool {
                      assert(false);
                      return false;
                    },
                },
                expr->rhs);
            if (!rhsComplete) {
              std::visit(visitor{expression, expr->rhs}, expr->rhs);
            }
          }
        }
      }
      void operator()(std::monostate&) {
        // assert(false);
        parent = expression;
        // return lhs + rhs;
      }
      Expr& expression;
      Expr& parent;
    };

    std::visit(visitor{newExpr, curr}, curr);
  }

  bool evaluate(const Expr& expr, std::uint64_t timestamp, std::int64_t value) {
    struct visitor {
      visitor(std::uint64_t timestamp, std::uint64_t value): ts(timestamp), val(value) {}
      std::uint64_t operator()(const UnsignedLiteralExpr& expr) {
        return expr.value;
      }

      std::uint64_t operator()(const SignedLiteralExpr& expr) {
        return expr.value;
      }

      std::uint64_t operator()([[maybe_unused]] const TimestampLiteral& expr) {
        return ts;
      }

      std::uint64_t operator()([[maybe_unused]] const ValueLiteral& expr) {
        return val;
      }

      std::uint64_t operator()(const box<EqExpr>& expr) {
        auto lhs = std::visit(*this, expr->lhs);
        auto rhs = std::visit(*this, expr->rhs);
        return lhs == rhs;
      }

      std::uint64_t operator()(const box<OrExpr>& expr) {
        auto lhs = std::visit(*this, expr->lhs);
        auto rhs = std::visit(*this, expr->rhs);
        return lhs || rhs;
      }

      std::uint64_t operator()(const box<AndExpr>& expr) {
        auto lhs = std::visit(*this, expr->lhs);
        auto rhs = std::visit(*this, expr->rhs);
        return lhs && rhs;
      }

      std::uint64_t operator()(const box<GtExpr>& expr) {
        auto lhs = std::visit(*this, expr->lhs);
        auto rhs = std::visit(*this, expr->rhs);
        return lhs > rhs;
      }
      std::uint64_t operator()(const box<LtExpr>& expr) {
        auto lhs = std::visit(*this, expr->lhs);
        auto rhs = std::visit(*this, expr->rhs);
        return lhs < rhs;
      }
      std::uint64_t operator()(const box<AddExpr>& expr) {
        auto lhs = std::visit(*this, expr->lhs);
        auto rhs = std::visit(*this, expr->rhs);
        return lhs + rhs;
      }
      std::uint64_t operator()(const std::monostate&) {
        assert(false);
        // return lhs + rhs;
      }

    private:
      std::uint64_t ts;
      std::uint64_t val;
    };

    return std::visit(visitor{timestamp, static_cast<uint64_t>(value)}, expr);
  }

} // namespace SeriesQuery

#endif // __QUERY
