#ifndef __QUERY_PLANNER
#define __QUERY_PLANNER

#include <lexy/callback.hpp>
#include <lexy/dsl.hpp>
#include <string>

// https://forcedotcom.github.io/phoenix/
// https://godbolt.org/z/rfMs1x4xn
// Influx query:
// SELECT max(usage_user) from cpu where (hostname = 'host_1') and time >= '2016-01-01T18-24-41Z' and time <
// '2016-01-01T19-24-41Z' group by time(1m)

// TODO
// This file should implement a query parser and planner.
// For now due to time constraints we are shelving this in favor or having something
// more simple and straightforward in order to show off the performance of the system.
namespace grammar {
  namespace dsl = lexy::dsl;

  struct table {
        static constexpr auto rule =
            dsl::identifier(dsl::ascii::alpha_digit_underscore) |
            dsl::lit_c<'*'>;
        static constexpr auto value = lexy::as_string<std::string>;
    };

    struct select_expression {
        static constexpr auto rule =
            LEXY_LIT("select") + dsl::lit_c<' '> + dsl::p<table>;
        static constexpr auto value = lexy::as_string<std::string>;
    };

    struct table_expression {
        static constexpr auto rule =
            LEXY_LIT("from") + dsl::lit_c<' '> + dsl::p<table>;
    };

    struct or_condition : public lexy::expression_production {
        struct compare : dsl::infix_op_left {
            static constexpr auto op =
                dsl::op(LEXY_LIT("!=")) / dsl::op(LEXY_LIT("<>")) /
                dsl::op(LEXY_LIT("<=")) / dsl::op(LEXY_LIT(">=")) /
                dsl::op(dsl::lit_c<'<'>) / dsl::op(dsl::lit_c<'>'>) /
                dsl::op(dsl::lit_c<'='>);
            using operand = dsl::atom;
        };

        struct and_condition {
            // static constexpr auto rule =
            // dsl::identifier(dsl::ascii::alpha_digit_underscore) +
            // dsl::p<compare> +
            // dsl::identifier(dsl::ascii::alpha_digit_underscore);
        };

        struct condition {
            // static constexpr auto rule =
        };

        struct operation : dsl::infix_op_left {
            static constexpr auto op = dsl::op(LEXY_LIT("or"));
            using operand = compare;
        };

        static constexpr auto whitespace = dsl::ascii::space;
        static constexpr auto atom = dsl::ascii::alpha_digit_underscore;
    };

    struct where_expression {
        static constexpr auto rule = LEXY_LIT("where") + dsl::p<or_condition>;
    };

    struct select {
        static constexpr auto rule =
            dsl::p<select_expression> + dsl::lit_c<' '> +
            dsl::p<table_expression> +
            dsl::opt(dsl::lit_c<' '> >> dsl::p<where_expression>);
    };

    static constexpr auto rule = dsl::p<select>;
}; // namespace grammar

#endif // __QUERY_PLANNER
