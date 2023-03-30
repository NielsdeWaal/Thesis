#ifndef __QUERY_PLANNER
#define __QUERY_PLANNER

#include <lexy/callback.hpp>
#include <lexy/dsl.hpp>
#include <string>

// https://forcedotcom.github.io/phoenix/
// Influx query: 
// SELECT max(usage_user) from cpu where (hostname = 'host_1') and time >= '2016-01-01T18-24-41Z' and time < '2016-01-01T19-24-41Z' group by time(1m)

namespace grammar {
  namespace dsl = lexy::dsl;

  struct table {
        static constexpr auto rule = dsl::identifier(dsl::ascii::alpha_digit_underscore);
        static constexpr auto value = lexy::as_string<std::string>;
    };

    struct expression {
        static constexpr auto rule = LEXY_LIT("select");
    };

    static constexpr auto rule = dsl::p<expression> + dsl::lit_c<' '> + dsl::p<table>;
}

#endif // __QUERY_PLANNER
