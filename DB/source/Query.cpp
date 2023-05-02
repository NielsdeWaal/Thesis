#include "Query.hpp"
#include <charconv>
#include <variant>

void QueryManager::Parse(std::string_view buf) {
  while (!buf.empty()) {
    auto [token, consumed, eoe] = next_token(buf);
    // mLogger->warn("Read: {}, list start: {}", token, listStart);

    using namespace std::string_view_literals;
    if (token == "->>"sv) {
      // mExpressions.emplace_back("QueryStartExpr");
      mExpressions.emplace_back(QueryExpression{});
    } else if (token == "index"sv) {
      // mExpressions.emplace_back("IndexSelector");
      mExpressions.emplace_back(IndexExpression{});
    } else if (token == "tag"sv) {
      // mExpressions.emplace_back("TagSelector");
    } else if (token == "list"sv) {
      mExpressions.emplace_back(ListExpression{});
    } else if (auto [intVal, isInt] = integer(token); isInt) {
      mExpressions.emplace_back(Integer{intVal});
    } else {
      // if(std::holds_alternative<IndexExpression>(mExpressions.back())) {
      //   // mExpressions.emplace_back(token);
      //   IndexExpression& expr = std::get<IndexExpression>(mExpressions.back());
      //   expr.AddIndex(token);
      //  } else if (std::holds_alternative<ListExpression>(mExpressions.back())) {
      //   std::get<ListExpression>(mExpressions.back()).mListItems.push_back(token);
      // }
    }

    mParseStack.emplace(&(mExpressions.back()));
    EvaluateStack(mParseStack, mScopedVars);
    buf.remove_prefix(consumed);
  }
  // for(const auto& expr : mExpressions) {
  //   mLogger->warn("parsed: {}", expr);
  // }

  mExpressions.clear();
}

inline std::tuple<std::string_view, std::size_t, bool> QueryManager::next_token(std::string_view buff) {
  int start_index = 0;
  auto consumed = 0ul;
  for (auto ch : buff) {
    assert(ch != ')');
    if (ch != ' ' && ch != '(' /*&& ch != '\''*/) {
      break;
    }
    ++consumed;
    ++start_index;
  }
  buff.remove_prefix(start_index);
  // mLogger->error("{}", buff);
  // if(buff.starts_with('(')) {
  //   list = true;
  // }

  int end_index = 0;
  bool end_of_expr = false;
  for (auto ch : buff) {
    ++consumed;
    if (ch == ' ')
      break;
    if (ch == ')') {
      end_of_expr = true;
      break;
    }
    ++end_index;
  }

  for (auto ch : buff.substr(end_index + 1)) {
    if (ch != ' ' && ch != ')')
      break;
    ++consumed;
  }
  buff.remove_suffix(buff.size() - end_index);

  return {buff, consumed, end_of_expr};
}

inline std::tuple<int, bool> QueryManager::integer(std::string_view token) {
  int result;
  auto [ptr, ec] = std::from_chars(token.begin(), token.end(), result);
  if(ptr == token.end() && ec == std::errc()) {
    return {result, true};
  }
  return {result, false};
}

void QueryManager::EvaluateStack(
    std::stack<Expression*>& stack,
    std::vector<std::unordered_map<std::string_view, std::string>> variables) {
  while ((stack.size() > 1)) { // TODO check if top level is valid
    auto* expr = stack.top();
    // mLogger->warn("Evaluating: {}", *expr);
    if(std::holds_alternative<IndexExpression>(expr->expr)) {
      mLogger->info("Resolving {}", fmt::join(std::get<IndexExpression>(expr->expr).GetTarget(), ","));
    } else if (std::holds_alternative<ListExpression>(expr->expr)) {
      mLogger->info("In list");
    }
    stack.pop();
    stack.top()->Add(*expr);
  }
}
