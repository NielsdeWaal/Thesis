#include <cstdint>
#define ANKERL_NANOBENCH_IMPLEMENT
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "src/TimeTree.hpp"

#include <doctest.h>
#include <list>
#include <nanobench.h>

#define GEN_INSERT_TEST(SIZE)                                                                                          \
  TimeTree<SIZE> tree##SIZE;                                                                                           \
  bench.minEpochIterations(1000000).performanceCounters(true).run("Arity: " #SIZE " insertion", [&] {                  \
    tree##SIZE.Insert(start, end, 0);                                                                                  \
    ++start;                                                                                                           \
    ++end;                                                                                                             \
  });                                                                                                                  \
  start = 0;                                                                                                           \
  end = 0;

#define GEN_QUERY_TEST(SIZE)                                                                                           \
  SUBCASE("Arity of " #SIZE) {                                                                                         \
    std::ofstream output("QueryArity" #SIZE);                                                                          \
    ankerl::nanobench::Bench bench;                                                                                    \
    bench.title("Size: " #SIZE).unit("Query");                                                                         \
    bench.performanceCounters(true);                                                                                   \
    std::list<int> tmp;                                                                                                \
    TimeTree<SIZE> tree;                                                                                               \
    uint64_t start = 1;                                                                                                \
    uint64_t end = 1;                                                                                                  \
    while (start != 1'000'000) {                                                                                       \
      if (start % 100 == 0) {                                                                                          \
        for (int i = 0; i < 1'000; ++i) {                                                                              \
          tmp.push_back(i);                                                                                            \
        }                                                                                                              \
      }                                                                                                                \
      tree.Insert(start, end, 0);                                                                                      \
      ++start;                                                                                                         \
      ++end;                                                                                                           \
    }                                                                                                                  \
    bench.run("Arity: " #SIZE " lookup 10", [&] {                                                                      \
      auto res = tree.Query(10, 20);                                                                                   \
      ankerl::nanobench::doNotOptimizeAway(res);                                                                       \
    });                                                                                                                \
    bench.run("Arity: " #SIZE " lookup 1k", [&] {                                                                      \
      auto res = tree.Query(1'000, 2'000);                                                                             \
      ankerl::nanobench::doNotOptimizeAway(res);                                                                       \
    });                                                                                                                \
    bench.run("Arity: " #SIZE " lookup 10k", [&] {                                                                     \
      auto res = tree.Query(10'000, 20'000);                                                                           \
      ankerl::nanobench::doNotOptimizeAway(res);                                                                       \
    });                                                                                                                \
    bench.run("Arity: " #SIZE " lookup 100k", [&] {                                                                    \
      auto res = tree.Query(100'000, 200'000);                                                                         \
      ankerl::nanobench::doNotOptimizeAway(res);                                                                       \
    });                                                                                                                \
    bench                                                                                                              \
        .run(                                                                                                          \
            "Arity: " #SIZE " lookup 1M",                                                                              \
            [&] {                                                                                                      \
              auto res = tree.Query(1, 1'000'000);                                                                     \
              ankerl::nanobench::doNotOptimizeAway(res);                                                               \
            })                                                                                                         \
        .render(ankerl::nanobench::templates::json(), output);                                                         \
  };

TEST_CASE("Insertion Bench") {
  std::ofstream output("InsertBench");
  ankerl::nanobench::Bench bench;
  bench.title("Insertion benchmark").unit("Insert");
  bench.performanceCounters(true);
  uint64_t start = 1;
  uint64_t end = 1;

  GEN_INSERT_TEST(2);
  GEN_INSERT_TEST(22);
  GEN_INSERT_TEST(42);
  GEN_INSERT_TEST(62);
  GEN_INSERT_TEST(82);
  GEN_INSERT_TEST(102);
  GEN_INSERT_TEST(122);
  GEN_INSERT_TEST(142);
  GEN_INSERT_TEST(162);
  GEN_INSERT_TEST(182);
  GEN_INSERT_TEST(202);
  GEN_INSERT_TEST(222);
  GEN_INSERT_TEST(242);
  GEN_INSERT_TEST(262);
  GEN_INSERT_TEST(282);
  GEN_INSERT_TEST(302);
  GEN_INSERT_TEST(322);
  GEN_INSERT_TEST(342);
  GEN_INSERT_TEST(362);
  GEN_INSERT_TEST(382);
  GEN_INSERT_TEST(402);
  GEN_INSERT_TEST(422);
  GEN_INSERT_TEST(442);
  GEN_INSERT_TEST(462);
  GEN_INSERT_TEST(482);
  GEN_INSERT_TEST(502);
  GEN_INSERT_TEST(522);
  GEN_INSERT_TEST(542);
  GEN_INSERT_TEST(562);
  GEN_INSERT_TEST(582);
  GEN_INSERT_TEST(602);
  GEN_INSERT_TEST(622);
  GEN_INSERT_TEST(642);
  GEN_INSERT_TEST(662);
  GEN_INSERT_TEST(682);
  GEN_INSERT_TEST(702);
  GEN_INSERT_TEST(722);
  GEN_INSERT_TEST(742);
  GEN_INSERT_TEST(762);
  GEN_INSERT_TEST(782);
  GEN_INSERT_TEST(802);
  GEN_INSERT_TEST(822);
  GEN_INSERT_TEST(842);
  GEN_INSERT_TEST(862);
  GEN_INSERT_TEST(882);
  GEN_INSERT_TEST(902);
  GEN_INSERT_TEST(922);
  GEN_INSERT_TEST(942);
  GEN_INSERT_TEST(962);
  GEN_INSERT_TEST(982);
  GEN_INSERT_TEST(1002);
  GEN_INSERT_TEST(1022);

  bench.render(ankerl::nanobench::templates::json(), output);
}

TEST_CASE("Lookup Bench") {
  GEN_QUERY_TEST(8);
  GEN_QUERY_TEST(16);
  GEN_QUERY_TEST(32);
  GEN_QUERY_TEST(64);
  GEN_QUERY_TEST(128);
  GEN_QUERY_TEST(256);
  GEN_QUERY_TEST(512);
  GEN_QUERY_TEST(1024);
}