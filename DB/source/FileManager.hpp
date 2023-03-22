#ifndef __FILE_MANAGER
#define __FILE_MANAGER

#include <fmt/core.h>

#include "EventLoop.h"
#include "File.h"


// NOTES
// Instead of flushing the data passed to the manager, maybe keep the memtables
// together with the files, this way the user will have to wait untill a memtable
// becomes free

// Another option is to keep a memtable in memory for each timeseries
// This eases the problem of possibly overwriting data while the data is being written 
// to disk, however this does come at the expense of memory consumption

enum class FileStatus : std::uint8_t {
  NO_FREE_FILE = 0,
  SUCCEEDED,
};

class FileManager {
public:
  FileManager(EventLoop::EventLoop& ev)
  : mEv(ev)
  {
    mLogger = mEv.RegisterLogger("FileManger");
  }

  EventLoop::uio::task<> SetDataFiles(std::size_t count) {
    mAvailable.reserve(count);
    mFiles.reserve(count);
    for(int i = 0; i < count; ++i) {
      mLogger->info("Opening file nodes{}.dat", i);
      mFiles.emplace_back(mEv);
      co_await mFiles[i].file.OpenAt(fmt::format("nodes{}.dat", i));
      mAvailable.push_back(i);
    }

    co_return;
  }

  EventLoop::uio::task<FileStatus> InsertWrite(EventLoop::DmaBuffer& buf) {
    if(mAvailable.size() == 0) {
      co_return FileStatus::NO_FREE_FILE;
    }
    co_return FileStatus::SUCCEEDED;
  }

private:
  EventLoop::uio::task<> WaitForCompletion(EventLoop::DmaBuffer& buf) {
    std::size_t index = mAvailable.back();
    mLogger->info("Inserting write to file: {}", index);
    mAvailable.pop_back();
    mFiles[index].offset += buf.GetSize();
    co_await mFiles[index].file.Append(buf);
    mAvailable.push_back(index);
  }

private: 
  struct File {
    File(EventLoop::EventLoop& ev) : file(ev){}
    AppendOnlyFile file;
    std::size_t offset{0};
  };
  EventLoop::EventLoop& mEv;
  std::shared_ptr<spdlog::logger> mLogger;
  std::vector<File> mFiles;
  std::vector<std::size_t> mAvailable;
};

#endif // __FILE_MANAGER
