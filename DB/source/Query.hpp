#ifndef __QUERY
#define __QUERY

#include "DmaBuffer.h"
#include "EventLoop.h"
#include "UringCommands.h"

#include <cstdint>
#include <memory>
#include <regex>
#include <spdlog/spdlog.h>
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
// namespace SeriesQuery {
//   template<typename T> class box {
//     // Wrapper over unique_ptr.
//     std::unique_ptr<T> _impl;

//   public:
//     // Automatic construction from a `T`, not a `T*`.
//     box(T&& obj): _impl(new T(std::move(obj))) {}
//     box(const T& obj): _impl(new T(obj)) {}

//     // Copy constructor copies `T`.
//     box(const box& other): box(*other._impl) {}
//     box& operator=(const box& other) {
//       *_impl = *other._impl;
//       return *this;
//     }

//     // unique_ptr destroys `T` for us.
//     ~box() = default;

//     // Access propagates constness.
//     T& operator*() {
//       return *_impl;
//     }
//     const T& operator*() const {
//       return *_impl;
//     }

//     T* operator->() {
//       return _impl.get();
//     }
//     const T* operator->() const {
//       return _impl.get();
//     }
//   };

//   struct UnsignedLiteralExpr {
//     UnsignedLiteralExpr(std::uint64_t val): value(val) {}
//     std::uint64_t value;
//   };

//   struct SignedLiteralExpr {
//     SignedLiteralExpr(std::int64_t val): value(val) {}
//     std::int64_t value;
//   };

//   using Expr = std::variant<
//       UnsignedLiteralExpr,
//       SignedLiteralExpr,
//       box<struct EqExpr>,
//       box<struct OrExpr>,
//       box<struct AndExpr>,
//       box<struct GtExpr>,
//       box<struct LtExpr>>;

//   struct EqExpr {
//     Expr lhs, rhs;
//   };

//   struct OrExpr {
//     Expr lhs, rhs;
//   };

//   struct AndExpr {
//     Expr lhs, rhs;
//   };

//   struct GtExpr {
//     Expr lhs, rhs;
//   };

//   struct LtExpr {
//     Expr lhs, rhs;
//   };

//   bool evaluate(const Expr& expr) {
//     struct visitor {
//       std::uint64_t operator()(const UnsignedLiteralExpr& expr) {
//         return expr.value;
//       }

//       std::uint64_t operator()(const SignedLiteralExpr& expr) {
//         return expr.value;
//       }

//       std::uint64_t operator()(const box<EqExpr>& expr) {
//         auto lhs = std::visit(*this, expr->lhs);
//         auto rhs = std::visit(*this, expr->rhs);
//         return lhs == rhs;
//       }

//       std::uint64_t operator()(const box<OrExpr>& expr) {
//         auto lhs = std::visit(*this, expr->lhs);
//         auto rhs = std::visit(*this, expr->rhs);
//         return lhs || rhs;
//       }

//       std::uint64_t operator()(const box<AndExpr>& expr) {
//         auto lhs = std::visit(*this, expr->lhs);
//         auto rhs = std::visit(*this, expr->rhs);
//         return lhs && rhs;
//       }

//       std::uint64_t operator()(const box<GtExpr>& expr) {
//         auto lhs = std::visit(*this, expr->lhs);
//         auto rhs = std::visit(*this, expr->rhs);
//         return lhs > rhs;
//       }
//       std::uint64_t operator()(const box<LtExpr>& expr) {
//         auto lhs = std::visit(*this, expr->lhs);
//         auto rhs = std::visit(*this, expr->rhs);
//         return lhs < rhs;
//       }
//     };

//     return std::visit(visitor{}, expr);
//   }

// } // namespace SeriesQuery

class QueryManager {
public:
  QueryManager(EventLoop::EventLoop& ev): mEv(ev) {
    mLogger = mEv.RegisterLogger("QueryManager");
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

  void Parse(std::string_view buf);

private:
  class ExpressionInterface {};
  class Integer {
  public: 
    Integer(int val): value(val) {}
  private: 
    int value;
  };

  class IndexExpression {
  public:
    void AddIndex(std::string_view index) {
      mTargetIndex.push_back(index);
    }
    void Add(std::string_view index) {
      mTargetIndex.push_back(index);
    }

    std::vector<std::string_view> GetTarget() const {
      return mTargetIndex;
    }

  private:
    std::vector<std::string_view> mTargetIndex;
  };

  struct ListExpression {
    void Add(std::string_view val) {
      mListItems.push_back(val);
    }
    void Add(Integer val) {
      mListItems.push_back(val);
    }
    std::vector<std::variant<std::string_view, Integer>> mListItems;
  };

  class QueryExpression {
  public:
    void SetIndex(std::string_view index) {
      mTargetIndex = index;
    }

    void add(Integer val) {
      mTargetIndex = val;
    }

  private:
    std::variant<std::string_view, Integer> mTargetIndex;
  };


  struct Expression {
    using ExpressionT = std::variant<QueryExpression, IndexExpression, ListExpression, Integer>;

    Expression(const ExpressionT& arg) : expr(arg) {}

    void Add(ExpressionT& arg) {
      std::visit([&arg](auto& expr){expr.add(arg);});
    }

    ExpressionT expr;
  };

  inline std::tuple<std::string_view, std::size_t, bool> next_token(std::string_view buff);
  inline std::tuple<int, bool> integer(std::string_view token);

  void EvaluateStack(
      std::stack<Expression*>& stack,
      std::vector<std::unordered_map<std::string_view, std::string>> variables);

  EventLoop::EventLoop& mEv;

  std::deque<Expression> mExpressions;
  std::stack<Expression*> mParseStack;
  std::vector<std::unordered_map<std::string_view, std::string>> mScopedVars;

  std::shared_ptr<spdlog::logger> mLogger;
};


#endif // __QUERY
