#include <cstdint>
#include <deque>
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include "doctest.h"
#include "src/TimeTree.hpp"

TEST_CASE("Basic tree insertion tests") {
  SUBCASE("Arity of 2") {
    TimeTree<2> tree;
    CHECK(tree.GetHeight() == 1);

    auto startingRoot = tree.GetRoot();

    tree.Insert(1, 1, 0);
    CHECK(tree.GetHeight() == 1);
    tree.Insert(2, 2, 0);
    CHECK(tree.GetHeight() == 1);
    CHECK(startingRoot->GetChildCount() == 2);
    CHECK(tree.GetNumberLeafs() == 1);

    tree.Insert(3, 3, 0);
    auto newRoot = tree.GetRoot();

    tree.PrintTree();

    CHECK(tree.GetHeight() == 2);
    CHECK(tree.GetNumberLeafs() == 2);
    CHECK(startingRoot != newRoot);
    CHECK(newRoot->GetChildCount() == 2);

    auto iter = tree.GetLeafsIterator();
    TimeTreeNode<2>* start = *iter;
    CHECK((*iter)->GetChildCount() == 2);
    CHECK(start->GetLink() == (*std::next(iter)));
    CHECK((*std::next(iter))->GetChildCount() == 1);

    uint64_t tester = 1;
    for (TimeTreeNode<2>& node : tree) {
      CHECK(node.GetNodeStart() != 0);
      CHECK(node.GetNodeEnd() != 0);
      const std::span<TimeRange_t> range = node.GetData();
      for (const TimeRange_t r : range) {
        CHECK(r.start == tester);
        CHECK(r.end == tester);
        CHECK(r.ptr == 0);
        ++tester;
      }
      // for (TimeRange_t item : *node) {
      //   CHECK(item.start != 0);
      //   CHECK(item.end != 0);
      //   CHECK(item.ptr == 0);
      // }
    }

    // std::size_t count = (*iter)->GetChildCount();
    // for (std::size_t i = 0; i != count; ++i) {
    //   for (const auto& ptr : (*iter)->GetData()) {
    //     CHECK(ptr.start != 0);
    //     CHECK(ptr.end != 0);
    //     CHECK(ptr.ptr == 0);
    //   }
    //   ++iter;
    // }

    tree.Insert(4, 4, 0);
    tree.Insert(5, 5, 0);
    CHECK(tree.GetHeight() == 3);
    tree.Insert(6, 6, 0);
    tree.Insert(7, 7, 0);

    tree.Insert(8, 8, 0);
    tree.Insert(9, 9, 0);
    tree.Insert(10, 10, 0);
    tree.Insert(11, 11, 0);
    tree.PrintTree();
  };

  // SUBCASE("Other tests") {
  //   TimeTree<2> tree;
  //   CHECK(tree.GetHeight() == 1);

  //   tree.Insert(1, 1, 0);
  //   CHECK(tree.GetHeight() == 1);
  //   tree.Insert(2, 2, 0);
  //   CHECK(tree.GetHeight() == 1);

  //   // Value has already been inserted
  //   tree.Insert(1, 1, 0);
  //   CHECK(tree.GetHeight() == 1);
  // };

  SUBCASE("Arity of 8") {
    TimeTree<8> tree;
    CHECK(tree.GetHeight() == 1);

    auto startingRoot = tree.GetRoot();

    for (int i = 1; i <= 8; ++i) {
      tree.Insert(i, i, 0);
    }
    CHECK(tree.GetHeight() == 1);
    CHECK(startingRoot->GetChildCount() == 8);
    CHECK(tree.GetNumberLeafs() == 1);

    for (int i = 9; i <= 16; ++i) {
      tree.Insert(i, i, 0);
    }
    CHECK(tree.GetHeight() == 2);
    CHECK(tree.GetNumberLeafs() == 2);

    uint64_t tester = 1;
    for (TimeTreeNode<8>& node : tree) {
      CHECK(node.GetNodeStart() != 0);
      CHECK(node.GetNodeEnd() != 0);
      const std::span<TimeRange_t> range = node.GetData();
      for (const TimeRange_t r : range) {
        CHECK(r.start == tester);
        CHECK(r.end == tester);
        CHECK(r.ptr == 0);
        ++tester;
      }
      // for (TimeRange_t item : *node) {
      //   CHECK(item.start != 0);
      //   CHECK(item.end != 0);
      //   CHECK(item.ptr == 0);
      // }
    }
    // for (TimeTreeNode<8>* node : tree) {
    //   CHECK(node->GetNodeStart() != 0);
    //   CHECK(node->GetNodeEnd() != 0);
    //   for (TimeRange_t item : *node) {
    //     CHECK(item.start != 0);
    //     CHECK(item.end != 0);
    //     CHECK(item.ptr == 0);
    //   }
    // }
    // auto iter = tree.GetLeafsIterator();
    // std::size_t count = (*iter)->GetChildCount();
    // for (std::size_t i = 0; i != count; ++i) {
    //   for (const auto& ptr : (*iter)->GetData()) {
    //     CHECK(ptr.start != 0);
    //     CHECK(ptr.end != 0);
    //     CHECK(ptr.ptr == 0);
    //   }
    //   ++iter;
    // }
  };
}

TEST_CASE("Basic node tests") {
  auto leafNode = TimeTreeNode<2>(true, 0, 10, 0);
  CHECK(leafNode.GetNodeStart() == 0);
  CHECK(leafNode.GetNodeEnd() == 10);
  CHECK(leafNode.Insert(12, 11, 0) == tl::unexpected(Errors_e::INVALID_TIME_RANGE));
  // CHECK(leafNode.Insert(9, 11, 0) == tl::unexpected(Errors_e::INVALID_TIME_RANGE));

  auto treeNode = TimeTreeNode<2>(false, 0, 10, 0);
  CHECK(treeNode.Insert(11, 12, 0) == tl::unexpected(Errors_e::NON_LEAF_PTR_INSERT));
  CHECK(treeNode.GetNodeStart() == 0);
  CHECK(treeNode.GetNodeEnd() == 10);
}

TEST_CASE("Query tests") {
  SUBCASE("Simple queries, ary: 2") {
    TimeTree<2> tree;
    for (int i = 1; i <= 16; ++i) {
      tree.Insert(i, i, 0);
    }

    tree.PrintTree();

    CHECK(tree.Query(2, 1) == tl::unexpected(Errors_e::INVALID_TIME_RANGE));
    CHECK(tree.Query(20, 21) == tl::unexpected(Errors_e::RANGE_NOT_IN_DB));

    auto qRes = tree.Query(1, 1);
    CHECK(qRes.has_value() == true);
    std::vector<TimeRange_t> q = qRes.value();

    CHECK(q.size() == 1);

    qRes = tree.Query(1, 16);
    CHECK(qRes.has_value() == true);
    CHECK(qRes->size() == 16);
  };
  SUBCASE("Simple queries, ary: 4") {
    TimeTree<4> tree;
    for (int i = 1; i <= 16; ++i) {
      tree.Insert(i, i, 0);
    }

    tree.PrintTree();

    CHECK(tree.Query(2, 1) == tl::unexpected(Errors_e::INVALID_TIME_RANGE));
    CHECK(tree.Query(20, 21) == tl::unexpected(Errors_e::RANGE_NOT_IN_DB));

    auto qRes = tree.Query(1, 1);
    CHECK(qRes.has_value() == true);
    std::vector<TimeRange_t> q = qRes.value();

    CHECK(q.size() == 1);

    qRes = tree.Query(2, 13);
    CHECK(qRes.has_value() == true);
    CHECK(qRes->size() == 12);
  };
  SUBCASE("Sparse queries") {
    // TODO check that queries with an end in the middle of the node return
    // the right amount of ptrs.
    TimeTree<4> tree;
    for (int i = 10; i <= 100; i += 10) {
      tree.Insert(i, i + 5, 0);
    }

    tree.PrintTree();

    auto qRes = tree.Query(50, 60);
    CHECK(qRes.has_value() == true);
    CHECK(qRes->size() == 2);

    qRes = tree.Query(45, 55);
    CHECK(qRes != tl::unexpected(Errors_e::RANGE_NOT_IN_DB));
    CHECK(qRes.has_value() == true);
    CHECK(qRes->size() == 2);
  };
  SUBCASE("Big queries") {
    TimeTree<64> tree;
    for (int i = 1; i <= 100000; ++i) {
      tree.Insert(i, i, 0);
    }
    // auto i = GENERATE(random(1, 100000));
    // auto j = GENERATE(random(i, 100000 - i));

    // 10 samples, generated using fair dicerolls
    for (auto [a, b] : std::vector<std::pair<std::size_t, std::size_t>>{
             {6347, 84682},
             {20029, 36875},
             {7154, 39037},
             {48796, 49270},
             {25895, 49156},
             {26374, 48889},
             {39678, 43239},
             {48409, 51404},
             {8831, 72340},
             {10436, 65401}}) {
      auto res = tree.Query(a, b);
      CHECK(res.has_value());
      CHECK(res->size() == (b - a) + 1);
    }
    // res = tree.Query(6347, 84682);
    // CHECK(res.has_value());
  };
}

TEST_CASE("TimeTree leaf iterator") {
  TimeTree<4> tree;
  for (int i = 1; i <= 32; ++i) {
    tree.Insert(i, i, 0);
  }

  std::deque<std::deque<TimeTreeNode<4>*>>& nodes = tree.Data();
  nodes.at(0).erase(nodes.at(0).begin(), nodes.at(0).begin() + 4);
  nodes.at(1).front()->ConvertToLeaf();

  for (TimeTreeNode<4>& node : tree) {
    fmt::print("{} -> {}\n", node.GetNodeStart(), node.GetNodeEnd());
  }

  // auto it = tree.begin();
  // for (int i = 0; i < 4; ++i) {
  //   // it->ConvertToLeaf();
  //   it = std::next(it);
  // }
}

TEST_CASE("TimeTree aggregation") {
  TimeTree<4> tree;
  for (int i = 1; i <= 32; ++i) {
    tree.Insert(i, i, 0);
  }

  tree.PrintTree();

  std::vector<TimeRange_t> res;
  tree.Aggregate(5, res);
  CHECK(res.size() == 4);

  fmt::print("removed size: {}\n", res.size());
  for (TimeTreeNode<4> node : tree) {
    fmt::print("{} -> {}\n", node.GetNodeStart(), node.GetNodeEnd());
  }

  // 4 ptrs from the leaf node 1-4 have been removed, query should return 1 aggregated node
  auto qRes = tree.Query(1, 4);
  CHECK(qRes->size() == 1);

  qRes->clear();
  res.clear();

  // 1-4 is already aggregated, 5-8, 9-12, 13-16 have not yet been
  tree.Aggregate(16, res);
  fmt::print("removed size: {}\n", res.size());
  CHECK(res.size() == 12);

  // This should return aggregates of 1-4, 5-8, 9-12, 13-16
  qRes = tree.Query(1, 16);
  CHECK(qRes->size() == 4);

  tree.PrintTree();

  for (TimeTreeNode<4>& node : tree) {
    fmt::print("{} -> {}\n", node.GetNodeStart(), node.GetNodeEnd());
  }

  qRes->clear();
  res.clear();

  // Aggregates 1-4, 5-8, 9-12, 13-16 into 1-16
  tree.Aggregate(16, res);
  fmt::print("removed size: {}\n", res.size());
  CHECK(res.size() == 4);

  qRes = tree.Query(1, 16);
  CHECK(qRes->size() == 1);

  qRes->clear();
  qRes = tree.Query(1, 19);
  CHECK(qRes->size() == 4);

  tree.PrintTree();

  for (TimeTreeNode<4>& node : tree) {
    fmt::print("{} -> {}\n", node.GetNodeStart(), node.GetNodeEnd());
  }

  qRes->clear();
  res.clear();
  tree.Aggregate(21, res);
  fmt::print("removed size: {}\n", res.size());
  CHECK(res.size() == 4);
  tree.PrintTree();

  for (TimeTreeNode<4>& node : tree) {
    fmt::print("{} -> {}\n", node.GetNodeStart(), node.GetNodeEnd());
  }
}
