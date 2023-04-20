#ifndef __METADATA_HPP
#define __METADATA_HPP

#include "IngestionProtocol/proto.capnp.h"
#include "SeriesIndex.hpp"
#include "UringCommands.h"

#include <algorithm>
#include <string>
#include <regex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct TSTag {
  TSTag() = default;
  TSTag(std::string tsName, std::string tsValue): name(std::move(tsName)), value(std::move(tsValue)) {}
  std::string name;
  std::string value;

  bool operator==(const TSTag& rhs) const = default;
};

template<> struct std::hash<TSTag> {
  std::size_t operator()(TSTag const& tag) const noexcept {
    std::size_t h1 = std::hash<std::string>{}(tag.name);
    std::size_t h2 = std::hash<std::string>{}(tag.value);
    return h1 ^ (h2 << 1); // or use boost::hash_combine
  }
};

class InvertedIndex {
public:
  InvertedIndex() = default;

  // explicit InvertedIndex(std::unordered_map<TSTag, std::unordered_set<std::uint64_t>> mMapping)
  // : mMapping(std::move(mMapping)) {}

  void Insert(const TSTag& tag, const std::uint64_t index) {
    mMapping[tag].insert(index);
  }

  std::unordered_set<std::uint64_t> GetPostings(const TSTag& kv) {
    return mMapping[kv];
  }

private:
  std::unordered_map<TSTag, std::unordered_set<std::uint64_t>> mMapping;
};

class MetaData {
public:
  MetaData(EventLoop::EventLoop& ev) : mIdIndex(ev) {}

  void Configure() {
    // First start index and have it read the indexing files
    // Next, load tagset to index mapping from the index
  }

  EventLoop::uio::task<> Startup() {
    co_await mIdIndex.SetupFiles();
    assert(mIdIndex.StartupDone());
  }

  void ReloadIndexes() {
    std::regex delim{","};
    std::regex tagDelim{"="};
    for(auto [k,v] : mIdIndex.GetIndexMapping()) {
      std::vector<std::string> tags(std::sregex_token_iterator(k.begin(), k.end(), delim,-1), {});
      for(const std::string& tag: tags) {
        std::vector<std::string> kv(std::sregex_token_iterator(tag.begin(), tag.end(), tagDelim,-1), {});
        if(kv.size() > 1) {
          // spdlog::info("Mapped {} {} to {}", kv.front(), kv.at(1), v);
          mInvertedIndex.Insert({kv.front(), kv.at(1)}, v);
        } else {
          // spdlog::info("Metric: {}", kv.front());
          mMetricMap.insert({v, kv.front()});
        }
        // spdlog::info("Mapped {} to {}", tag, v);
      }
    }
   }

  // std::optional<std::uint64_t> GetIndex(proto::IdRequest::Reader& request) {
  // Create conanical stream name and search for index in mIdIndex
  // }

  // void Insert(const std::vector<std::pair<std::string, std::string>>& tags) {
  //   // mIdTagMap[testingCounter] = ;
  //   for (const std::pair<std::string, std::string>& tag : tags) {
  //     auto insertRes = mTags.insert({tag.first, tag.second});
  //     // if (!mIdTagMap.contains(testingCounter)) {
  //     //   mIdTagMap[testingCounter] = std::vector<std::reference_wrapper<TSTag>>{insertRes.first};
  //     // }
  //     mIdTagMap[testingCounter].push_back(&(*insertRes.first));
  //     mInvertedIndex.Insert({tag.first, tag.second}, testingCounter);
  //   }
  //   ++testingCounter;
  // }

  std::uint64_t GetIndex(const proto::IdRequest::Reader& request) {
    std::string name;
    name.reserve(255);

    auto tags = request.getTagSet();
    std::for_each(tags.begin(), tags.end(), [&](proto::Tag::Reader tag) {
      name.append(std::string{tag.getName().cStr()} + "=" + tag.getValue().cStr() + ",");
    });
    name.append(request.getMetric().cStr());

    // std::uint64_t index = mIdIndex.GetIndex();
    std::optional<std::uint64_t> index = mIdIndex.GetIndex(name);
    if(!index.has_value()) {
      index = mIdIndex.InsertSeries(name);
    }

    for(proto::Tag::Reader tag: request.getTagSet()) {
      mInvertedIndex.Insert({tag.getName().cStr(), tag.getValue().cStr()}, index.value());
    }
    
    return index.value();
  }

  /**
   * @brief Returns all metric streams which intersect with `tag`
   *
   */
  // std::optional<std::vector<std::uint64_t>> GetIntersect(const TSTag& tag) {}

  /**
   * @brief Query tag sets for streams containing one of the values for a tag
   */
  std::optional<std::vector<std::uint64_t>>
      QueryValues(const std::string& name, const std::vector<std::string>& value) {
    // Find streams which contain certain values for tags
    // std::unordered_set<std::uint64_t> postings = mInvertedIndex.GetPostings({name, value.at(0)});
    std::vector<std::uint64_t> res;
    std::vector<std::unordered_set<std::uint64_t>> postings;
    std::unordered_set<std::uint64_t> temp;
    for (const auto& val : value) {
      // std::unordered_set<std::uint64_t> postings = mInvertedIndex.GetPostings({name, val});
      postings.push_back(mInvertedIndex.GetPostings({name, val}));
    }

    if (postings.empty()) {
      return std::nullopt;
    }
    if (postings.size() == 1) {
      std::vector<std::uint64_t> vec;
      vec.assign(postings.front().begin(), postings.front().end());
      return vec;
    }

    auto it = postings.begin();
    std::advance(it, 1);
    std::unordered_set<std::uint64_t> start = setintersection(postings.front(), *it);
    // TODO Check if postings.begin here works correctly, might have to be changed to it
    auto result = std::reduce(postings.begin(), postings.end(), start, [&](auto acc, auto posting) {
      return setintersection(acc, posting);
    });
    res.assign(result.begin(), result.end());
    return res;
    // for(const auto& set : postings) {
    //   setintersection(set, , )
    // }

    // return std::vector<std::uint64_t>{postings.begin(), postings.end()};
  }

private:
  std::unordered_set<std::uint64_t>
      setintersection(std::unordered_set<std::uint64_t>& h1, std::unordered_set<std::uint64_t>& h2) {
    if (h1.size() > h2.size()) {
      return setintersection(h2, h1);
    }
    std::unordered_set<std::uint64_t> answer;
    // answer.clear();
    for (std::unordered_set<std::uint64_t>::iterator i = h1.begin(); i != h1.end(); i++) {
      if (h2.find(*i) != h2.end())
        answer.insert(*i);
    }
    return answer;
  }

  // FIXME a sorted vector combined with `set_intersection` provides better performance
  // std::vector<TSTag> mTagSet;
  // std::unordered_set<TSTag> mTags;
  // std::unordered_map<std::string, std::uint64_t> mMetricMap;
  std::unordered_map<std::uint64_t, std::string> mMetricMap;
  InvertedIndex mInvertedIndex;
  std::unordered_map<std::uint64_t, std::vector<const TSTag*>> mIdTagMap;
  Index mIdIndex;

  // int testingCounter{1};
};

#endif // __METADATA_HPP
