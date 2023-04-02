#ifndef __QUERY
#define __QUERY

#include "DmaBuffer.h"
#include "EventLoop.h"
#include "UringCommands.h"

#include <cstdint>
#include <memory>
#include <vector>

// TODO there is an opportunity to do coalessing on the reads here
// When multiple sequential reads are detected, these can be merged into larger reads
// for better efficiency. Might need some kind of 'max' setting dictation the maximum
// size of such a read.

struct IOOP {
  std::uint64_t pos;
  std::size_t len;
  EventLoop::DmaBuffer buf;
};

class Query: EventLoop::IUringCallbackHandler {
public:
  Query() = default;

  Query(EventLoop::EventLoop& ev, int fd, std::vector<IOOP>& ops) {
    mInFlightIO.reserve(ops.size());
    for (IOOP& op : ops) {
      std::unique_ptr<EventLoop::UserData> data = std::make_unique<EventLoop::UserData>();

      data->mCallback = this;
      data->mType = EventLoop::SourceType::Read;
      data->mInfo = EventLoop::READ{.fd = fd, .buf = op.buf.GetPtr(), .len = op.len, .pos = op.pos};

      ev.QueueStandardRequest(std::move(data));

      mInFlightIO.push_back(IOOP{.pos = op.pos, .len = op.len, .buf = std::move(op.buf)});
    }
    mIOCount = ops.size();
  }
  Query(EventLoop::EventLoop& ev, int fd, const std::vector<std::uint64_t>& addrs, std::size_t blockSize) {
    mInFlightIO.reserve(addrs.size());
    for (const std::uint64_t addr : addrs) {
      std::unique_ptr<EventLoop::UserData> data = std::make_unique<EventLoop::UserData>();
      EventLoop::DmaBuffer buf = ev.AllocateDmaBuffer(blockSize);

      data->mCallback = this;
      data->mType = EventLoop::SourceType::Read;
      data->mInfo = EventLoop::READ{.fd = fd, .buf = buf.GetPtr(), .len = blockSize, .pos = addr};

      ev.QueueStandardRequest(std::move(data));

      mInFlightIO.push_back(IOOP{.pos = addr, .len = blockSize, .buf = std::move(buf)});
    }
    mIOCount = addrs.size();
  }

  operator bool() const {
    return mDone;
  }

  void OnCompletion(EventLoop::CompletionQueueEvent& cqe, const EventLoop::UserData* data) override {
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

class QueryManager {};

namespace SeriesQuery {
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
  };

  struct LiteralExpr {
    std::uint64_t value;
  };

  using Expr = std::variant<LiteralExpr, box<struct EqExpr>, box<struct OrExpr>, box<struct AndExpr>, box<struct GtExpr>, box<struct LtExpr>>;

  struct EqExpr {
    Expr lhs, rhs;
  };

  struct OrExpr {
    Expr lhs, rhs;
  };

  struct AndExpr {
    Expr lhs, rhs;
  };

  struct GtExpr {
    Expr lhs, rhs;
  };

  struct LtExpr {
    Expr lhs, rhs;
  };

  bool evaluate(const Expr& expr) {
    struct visitor {
      std::uint64_t operator()(const LiteralExpr& expr) {
        return expr.value;
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
    };

    return std::visit(visitor{}, expr);
  }

} // namespace SeriesQuery

#endif // __QUERY
