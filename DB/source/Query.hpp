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

template<typename ReturnType> struct Expression {
  virtual ~Expression(){};
  virtual ReturnType operator()() const = 0;
};

template<typename ValueType> struct ValueToken: public Expression<ValueType> {
  ValueToken(const ValueType value): mValue(value) {}


  ValueType operator()() const {
    return mValue;
  }

public:
  ValueType mValue;
};

template<typename NumberType> struct NumberToken : public ValueToken<NumberType> {
  NumberToken(const NumberType value = {})
    : ValueToken<NumberType>(value) {}
};

template<typename LeftType, typename RightType, typename ResType> class OrExpression: public Expression<ResType> {
public:
  OrExpression() = default;

  bool operator()() {
    return (*this->mLeft)() || (*this->mRight)();
  }
};

template<typename LeftType, typename RightType, typename ResType> class AndExpression: public Expression<ResType> {
public:
  bool operator()() {
    return (*this->mLeft)() && (*this->mRight)();
  }
};

template<typename LeftType, typename RightType, typename ResType> class BinaryExpression: public Expression<ResType> {
public:
  template<class T1, class T2, class = decltype(LeftT(std::declval<T1&&>()), RightT(std::declval<T2&&>()), void())>
  BinaryExpression(Expression<LeftType>&& left = nullptr, Expression<RightType>&& right = nullptr)
  : mLeft(std::forward<T1>(left))
  , mRight(std::forward<T2>(right)) {}

  BinaryExpression(Expression<LeftType>* left = nullptr, Expression<RightType>* right = nullptr)
  : mLeft(left)
  , mRight(right) {}

public:
  Expression<LeftType>* mLeft;
  Expression<RightType>* mRight;
};

template<typename LeftType, typename RightType, typename ResType>
class lt: public BinaryExpression<LeftType, RightType, ResType> {
public:
  lt(Expression<LeftType>&& left, Expression<RightType>&& right)
  : BinaryExpression<LeftType, RightType, ResType>(left, right) {}
  lt(Expression<LeftType>* left, Expression<RightType>* right)
  : BinaryExpression<LeftType, RightType, ResType>(left, right) {}

  ResType operator()() const {
    return (*this->mLeft)() < (*this->mRight)();
  }
};

template<typename LeftType, typename RightType, typename ResType>
class gt: public BinaryExpression<LeftType, RightType, ResType> {
public:
  gt(Expression<LeftType>&& left, Expression<RightType>&& right)
  : BinaryExpression<LeftType, RightType, ResType>(left, right) {}
  gt(Expression<LeftType>* left, Expression<RightType>* right)
  : BinaryExpression<LeftType, RightType, ResType>(left, right) {}
  ResType operator()() const {
    return (*this->mLeft)() > (*this->mRight)();
  }
};

template<typename LeftType, typename RightType, typename ResType>
class eq: public BinaryExpression<LeftType, RightType, ResType> {
public:
  eq(Expression<LeftType>&& left, Expression<RightType>&& right)
  : BinaryExpression<LeftType, RightType, ResType>(left, right) {}

  eq(Expression<LeftType>* left, Expression<RightType>* right)
  : BinaryExpression<LeftType, RightType, ResType>(left, right) {}

  ResType operator()() const {
    return (*this->mLeft)() == (*this->mRight)();
  }
};

#endif // __QUERY
