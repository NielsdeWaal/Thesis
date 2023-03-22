// #include <source/EventLoop/EventLoop.h>
#include "TSC.h"
#include <charconv>
#include <iomanip>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>

#include <fmt/core.h>

#include <EventLoop.h>
#include <File.h>
#include <src/TimeTree.hpp>

#include "FileManager.hpp"

class Storage {
public:
  virtual ~Storage() = 0;
  virtual void InsertDataBlock(EventLoop::DmaBuffer) = 0;
  virtual EventLoop::DmaBuffer ReadDataBlock(std::uint64_t addr) = 0;
  virtual void InsertLogStatement(std::uint64_t start, std::uint64_t end,
                                  std::uint64_t pos) = 0;
};

class Handler //: public Common::IStreamSocketServerHandler
              //: Common::StreamSocketServer
{
public:
  explicit Handler(EventLoop::EventLoop &ev)
      : mEv(ev), mLogFile(mEv), mNodeFile(mEv), mTestFile(mEv),
        mFileManager(mEv) // , mSocket(mEv, this)
  {
    mLogger = mEv.RegisterLogger("FrogFishDB");
    // mFiles.reserve(4);
    func();
  }

  // std::vector<std::string_view>
  // split_at_any(const std::string_view input,
  //              const std::string_view delimiters) {
  //   std::vector<std::string_view> result;
  //   std::size_t last_pos = 0;
  //   for (std::size_t i = 0; i < input.size(); ++i) {
  //     if (delimiters.find(input[i]) != delimiters.npos) {
  //       result.push_back(input.substr(last_pos, i - last_pos));
  //       ++i;
  //       last_pos = i;
  //     }
  //   }

  //   return result;
  // }
  EventLoop::uio::task<> func() {
    // mFile.Create("/tmp/eventloop_file");
    // int a = co_await mFile.OpenAt("/tmp/eventloop_file");
    co_await mLogFile.OpenAt("./log.dat");
    co_await mNodeFile.OpenAt("./nodes.dat");
    co_await mFileManager.SetDataFiles(4);
    // std::size_t count = 4;
    // for(std::size_t i = 0; i < count; ++i) {
    //   mLogger->info("Opening file nodes{}.dat", i);
    //   mFiles.emplace_back(mEv);
    //   co_await mFiles[i].OpenAt(fmt::format("nodes{}.dat", i));
    // }

    // TODO create this file manually and fill with test data
    // co_await mTestFile.OpenAt("./test");
    auto data = LoadDataFile("../medium-plus-1");
    // for(const auto& dp : data) {
    //   mLogger->info("{} - {}", dp.timestamp, dp.value);
    // }

    std::size_t fileOffset{0};
    std::size_t logOffset{0};
    EventLoop::DmaBuffer buf = mEv.AllocateDmaBuffer(bufSize);
    EventLoop::DmaBuffer logBuf = mEv.AllocateDmaBuffer(512);

    std::size_t ctr{0};
    std::array<DataPoint, memtableSize> memtable{};
    // DataPointt* memtable = (DataPoint*)buf.GetPtr();
    // std::array<DataPoint,memtableSize>& memtable =
    // reinterpret_cast<std::array<DataPoint,
    // memtableSize>&>((DataPoint*)buf.GetPtr()); DataPoint* memtable =
    // (DataPoint*)buf.GetPtr();
    auto start = Common::MONOTONIC_CLOCK::Now();

    for (const auto &dp : data) {
      if (!mTrees.contains(dp.series)) {
        mLogger->info("New series {}, creaing trees", dp.series);
        mTrees[dp.series] = MetricTree{.ctr = 0};
      }
      auto& tree = mTrees[dp.series];
      // memtable.at(ctr) = dp;
      // memtable[ctr] = dp;
      tree.memtable[tree.ctr] = DataPoint{.timestamp = dp.timestamp, .value = dp.value};
      // ++ctr;
      tree.ctr += 1;
      if (tree.ctr == memtableSize) {
        mLogger->info("Flushing memtable for {} ({} - {})", dp.series, tree.memtable.front().timestamp,
                      tree.memtable.back().timestamp);
        // mLogger->info("Flushing memtable ({} - {})", memtable[0].timestamp,
        // memtable[memtableSize].timestamp);
        // std::memcpy(buf.GetPtr(), memtable.data(), bufSize);
        // co_await mNodeFile.Append(buf);

        EventLoop::DmaBuffer flushBuf = mEv.AllocateDmaBuffer(bufSize);
        std::memcpy(flushBuf.GetPtr(), tree.memtable.data(), bufSize);
        co_await mNodeFile.Append(flushBuf);

        // flushBuf.Free();

        // auto res = co_await mFileManager.InsertWrite(buf);
        // while (res != FileStatus::SUCCEEDED) {
        //   res = co_await mFileManager.InsertWrite(buf);
        // }
        // mTree.Insert(memtable.front().timestamp, memtable.back().timestamp,
        //              fileOffset);
        tree.tree.Insert(tree.memtable.front().timestamp, tree.memtable.back().timestamp, fileOffset);

        LogPoint *log = (LogPoint *)logBuf.GetPtr();
        log->start = tree.memtable.front().timestamp;
        log->end = tree.memtable.back().timestamp;
        log->offset = fileOffset;
        co_await mLogFile.WriteAt(logBuf, logOffset);

        // mTree.Insert(memtable[0].timestamp, memtable[memtableSize].timestamp,
        // fileOffset);
        ctr = 0;
        fileOffset += bufSize;
        logOffset += 512;
      }
    }

    // TODO flush non empty memtables
    // if (ctr != 0) {
    //   mLogger->info("Flushing remaining (entries: {}) memtable ({} - {})", ctr,
    //                 memtable.front().timestamp, memtable.at(ctr).timestamp);
    //   // mLogger->info("Flushing remaining (entries: {}) memtable ({} - {})",
    //   // ctr, memtable[0].timestamp, memtable[ctr].timestamp);
    //   std::memcpy(buf.GetPtr(), memtable.data(), ctr);
    //   co_await mNodeFile.Append(buf);
    //   // auto res = co_await mFileManager.InsertWrite(buf);
    //   // while (res != FileStatus::SUCCEEDED) {
    //   //   res = co_await mFileManager.InsertWrite(buf);
    //   // }
    //   mTree.Insert(memtable.front().timestamp, memtable.back().timestamp,
    //                fileOffset);

    //   LogPoint *log = (LogPoint *)logBuf.GetPtr();
    //   log->start = memtable.front().timestamp;
    //   log->end = memtable.at(ctr).timestamp;
    //   log->offset = fileOffset;
    //   co_await mLogFile.WriteAt(logBuf, logOffset);

    //   // mTree.Insert(memtable[0].timestamp, memtable[ctr].timestamp,
    //   // fileOffset);
    //   ctr = 0;
    //   fileOffset += bufSize;
    //   logOffset += 512;
    // }

    auto end = Common::MONOTONIC_CLOCK::Now();
    auto duration =
        Common::MONOTONIC_CLOCK::ToNanos(end - start) / 100 / 100 / 100;

    mLogger->info("Using memtable of size {}", memtableSize);

    mLogger->info("Ingestion took, {}ms, {} points, {} points/s", duration,
                  data.size(),                    //(
                  (data.size() / duration) * 1000 //) * 100 /// (duration / 100)
    );

    buf.Free();

    for (auto &[name, metric] : mTrees) {
      auto tree = metric.tree; 
      mLogger->info("Tree is height: {}", tree.GetHeight());
      for (TimeTreeNode<64> &node : tree) {
        mLogger->info("{} -> {} (links: {})", node.GetNodeStart(),
                      node.GetNodeEnd(), node.GetChildCount());
      }
    }

    // EventLoop::DmaBuffer testBuf = mEv.AllocateDmaBuffer(4096);
    // EventLoop::DmaBuffer testBuf{4096};
    // int ret = co_await mFile.Append(testBuf);
    // assert(ret == 4096);

    // EventLoop::DmaBuffer verificationBuf = mEv.AllocateDmaBuffer(4096);
    // ret = co_await mFile.ReadAt(verificationBuf, 0);
    // REQUIRE(ret == 4096);
    // EventLoop::DmaBuffer verBuf = co_await mFile.ReadAt(0, 4096);
    // assert(verBuf.GetSize() == 4096);

    // co_await mFile.Close();

    // verBuf.Free();

    mEv.Stop();
  }

  // Common::IStreamSocketHandler *OnIncomingConnection() final { return this; }

  // void OnConnected() final {}

  // void OnDisconnect([[maybe_unused]] Common::StreamSocket *conn) final {}

  // void OnIncomingData([[maybe_unused]] Common::StreamSocket *conn, char
  // *data,
  //                     std::size_t len) final {
  //   conn->Shutdown();
  // }

private:
  struct DataPoint {
    std::uint64_t timestamp;
    std::uint64_t value;
  };
  struct LogPoint {
    std::uint64_t start;
    std::uint64_t end;
    std::uint64_t offset;
  };

  static constexpr std::size_t bufSize{4096};
  static constexpr std::size_t memtableSize = bufSize / sizeof(DataPoint);
  
  struct MetricTree {
    TimeTree<64> tree;
    std::size_t ctr;
    std::array<DataPoint, memtableSize> memtable{};
  };
  
  struct IngestionPoint {
    std::string series;
    std::uint64_t timestamp;
    std::uint64_t value;
  };
  std::vector<IngestionPoint> LoadDataFile(const std::string &filename) {
    std::vector<IngestionPoint> resVec;
    resVec.reserve(10000);
    std::ifstream input(filename);
    for (std::string line; std::getline(input, line);) {
      // mLogger->info("{}", line);
      // auto vec = split_at_any(line, " ");
      // for (const auto str : vec) {
      IngestionPoint dp{};
      TokenGen tg{line, " "};
      // tg();
      // mLogger->info("{}", tg());
      std::string name{tg()};
      TokenGen values{tg(), ","};
      TokenGen point{values(), "="};
      point();
      while (point) {
        auto pt = point();
        pt.remove_suffix(1);
        int i;
        auto res = std::from_chars(pt.data(), pt.data() + pt.size(), i);
        if (res.ec == std::errc::invalid_argument) {
          assert(false);
        }
        // mLogger->info("{}", i);
        dp.value = i;
      }
      // mLogger->info("{}", tg());
      std::uint64_t ts;
      auto tsString = tg();
      auto res = std::from_chars(tsString.data(),
                                 tsString.data() + tsString.size(), ts);
      if (res.ec == std::errc::invalid_argument) {
        assert(false);
      }
      dp.series = std::move(name);
      dp.timestamp = ts;
      resVec.push_back(dp);
      // mLogger->info("{} -> {} - {}", dp.series, dp.timestamp, dp.value);
    }
    return resVec;
  }
  struct TokenGen {
    std::string_view sv;
    std::string_view del;
    operator bool() const { return !sv.empty(); }
    std::string_view operator()() {
      auto r = sv;
      const auto it = sv.find_first_of(del);
      if (it == std::string_view::npos) {
        sv = {};
      } else {
        r.remove_suffix(r.size() - it);
        sv.remove_prefix(it + 1);
      }
      return r;
    }
  };
  EventLoop::EventLoop &mEv;
  DmaFile mLogFile;
  AppendOnlyFile mNodeFile;
  FileManager mFileManager;
  std::shared_ptr<spdlog::logger> mLogger;
  // Common::StreamSocketServer mSocket;

  DmaFile mTestFile;
  // std::vector<AppendOnlyFile> mFiles;

  // TimeTree<64> mTree;
  std::unordered_map<std::string, MetricTree> mTrees;
};

int main() {
  EventLoop::EventLoop loop;
  loop.LoadConfig("FrogFish.toml");
  loop.Configure();

  Handler app(loop);

  loop.Run();

  return 0;
}
