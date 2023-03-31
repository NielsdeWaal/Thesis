#ifndef __QUERY_PLANNER
#define __QUERY_PLANNER

#include <lexy/callback.hpp>
#include <lexy/dsl.hpp>
#include <string>

// https://forcedotcom.github.io/phoenix/
// https://godbolt.org/z/vYqaM1xah
// Influx query:
// SELECT max(usage_user) from cpu where (hostname = 'host_1') and time >= '2016-01-01T18-24-41Z' and time <
// '2016-01-01T19-24-41Z' group by time(1m)

namespace grammar {
  namespace dsl = lexy::dsl;

  struct table {
    static constexpr auto rule = dsl::identifier(dsl::ascii::alpha_digit_underscore) | dsl::lit_c<'*'>;
    static constexpr auto value = lexy::as_string<std::string>;
  };

  struct select_expression {
    static constexpr auto rule = LEXY_LIT("select") + dsl::lit_c<' '> + dsl::p<table>;
    static constexpr auto value = lexy::as_string<std::string>;
  };

  struct table_expression {
    static constexpr auto rule = LEXY_LIT("from") + dsl::lit_c<' '> + dsl::p<table>;
  };

  struct where_expression {
    static constexpr auto rule = LEXY_LIT("where");
  };

  struct compare: dsl::infix_op_left {
    static constexpr auto op = dsl::op(dsl::lit_c<'*'>) / dsl::op(dsl::lit_c<'/'>);
    using operand = dsl::atom;
  };

  struct condition {
    // static constexpr auto rule =
  };

  struct and_condition {};

  struct or_condition {};

  struct select {
    static constexpr auto rule = dsl::p<select_expression> + dsl::lit_c<' '> + dsl::p<table_expression>
                                 + dsl::opt(dsl::lit_c<' '> >> dsl::p<where_expression>);
  };

  static constexpr auto rule = dsl::p<select>;
}; // namespace grammar

#endif // __QUERY_PLANNER
