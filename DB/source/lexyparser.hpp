#include <lexy/callback.hpp>
#include <lexy/dsl.hpp>
#include <string>
#include <variant>
#include <vector>


struct InfluxMValue {
  std::variant<std::int64_t, double> value;
};

struct InfluxKV {
  std::string name;
  std::variant<std::string, InfluxMValue> value;
  std::uint64_t index{0};
};

struct IMessage {
  std::string name;
  std::vector<InfluxKV> tags;
  std::vector<InfluxKV> measurements;
  std::uint64_t ts;
};

namespace grammar {
  namespace dsl = lexy::dsl;

  struct name {
    static constexpr auto rule
        // One or more alpha numeric characters, underscores or hyphens.
        = dsl::identifier(dsl::unicode::alnum / dsl::lit_c<'_'>);

    static constexpr auto value = lexy::as_string<std::string>;
  };

  struct tag_value {
    static constexpr auto rule = [] {
      auto head = dsl::ascii::alpha_digit_underscore / dsl::lit_c<'-'> / /*dsl::lit_c<' '> /*/ dsl::lit_c<'.'>;
      auto tail = dsl::ascii::alpha_digit_underscore / dsl::lit_c<'-'> / /*dsl::lit_c<' '> /*/ dsl::lit_c<'.'>;
      return dsl::identifier(head, tail);
    }();

    static constexpr auto value = lexy::as_string<std::string>;
  };

  struct measurement_value {
    struct Integer {
      struct integer {
        static constexpr auto rule = dsl::sign + dsl::integer<int>;
        static constexpr auto value = lexy::as_integer<int>;
      };

      static constexpr auto rule = [] {
        auto hex_integer = LEXY_LIT("0x") >> dsl::integer<int, dsl::hex>;
        auto regular_integer =
            dsl::peek(dsl::lit_c<'-'> / dsl::lit_c<'+'> / dsl::digit<>) >> dsl::p<integer> + dsl::lit_c<'i'>;
        return (hex_integer | regular_integer);
      }();

      static constexpr auto value = lexy::forward<int>;
    };

    struct Real {
      static constexpr auto rule = [] {
        auto integer_part = dsl::sign + dsl::digits<>;

        auto float_condition = dsl::peek_not(dsl::lit_c<'i'>);
        auto fraction = float_condition >> dsl::period >> dsl::digits<>;

        // We want either a fraction with an optional exponent, or an exponent.
        auto real_part = fraction;
        auto real_number = dsl::token(integer_part + real_part);
        return dsl::capture(real_number);
      }();

      static constexpr auto value =
          lexy::as_string<std::string> | lexy::callback<double>([](std::string&& str) { return std::stod(str); });
    };
    struct Number {
      static constexpr auto rule = dsl::p<Real> | dsl::p<Integer>;
      static constexpr auto value = lexy::construct<InfluxMValue>;
    };

    static constexpr auto rule = dsl::p<Number>;
    static constexpr auto value = lexy::forward<InfluxMValue>;
  };
  // struct measurement_value {
  //   struct number: lexy::token_production {
  //     struct integer: lexy::transparent_production {
  //       static constexpr auto rule =
  //           dsl::minus_sign + dsl::integer<std::int64_t>(dsl::digits<>.no_leading_zero()) + dsl::lit_c<'i'>;
  //       static constexpr auto value = lexy::as_integer<std::int64_t>;
  //     };

  //     struct floating: lexy::token_production {
  //       static constexpr auto rule = [] {
  //         auto integer = dsl::if_(dsl::lit_c<'-'>) + dsl::digits<>.no_leading_zero();
  //         auto fraction = dsl::lit_c<'.'> >> dsl::digits<>;
  //         return dsl::peek(dsl::lit_c<'-'> / dsl::digit<>)
  //                >> dsl::position + integer + dsl::if_(fraction) + dsl::position;
  //       }();

  //       static constexpr float atof(const char* first, const char* last) {
  //         // std::from_chars(const char*, const char*, float) is only
  //         // available from libc++ starting from LLVM 14 :(
  //         ( void ) (last);
  //         return ::atof(first);
  //       }

  //       static constexpr auto value =
  //           lexy::callback<float>([](const char* first, const char* last) { return atof(first, last); });
  //     };

  //     static constexpr auto rule = dsl::p<integer> | dsl::p<floating>;
  //     static constexpr auto value = lexy::construct<InfluxMValue>;
  //   };

  //   static constexpr auto rule = dsl::p<number>;
  //   static constexpr auto value = lexy::forward<InfluxMValue>;
  //   // static constexpr auto value = lexy::construct<
  //   // static constexpr auto rule = [] {
  //   //   auto head = dsl::ascii::digit;
  //   //   auto tail = dsl::ascii::alpha_digit;
  //   //   return dsl::identifier(head, tail);
  //   // }();

  //   // // static constexpr auto value = lexy::as_integer<int>;
  //   // static constexpr auto value = lexy::as_string<std::string>;
  // };

  template<typename ValType> struct kv {
    static constexpr auto rule = dsl::p<name> + dsl::lit_c<'='> + dsl::p<ValType>;
    static constexpr auto value = lexy::construct<InfluxKV>;
  };

  struct tags {
    static constexpr auto rule = [] {
      // auto item = dsl::capture(dsl::unicode::word / dsl::lit_c<'='> / dsl::ascii::alpha);
      auto item = dsl::p<kv<tag_value>>;
      auto sep = dsl::sep(dsl::lit_c<','>);
      return dsl::list(item, sep);
      // return dsl::ascii::alnum + dsl::comma + dsl::list(item, sep);
    }();

    static constexpr auto value = lexy::as_list<std::vector<InfluxKV>>;
  };

  struct measurements {
    static constexpr auto rule = [] {
      // auto item = dsl::capture(dsl::unicode::word / dsl::lit_c<'='> / dsl::ascii::alpha);
      auto item = dsl::p<kv<measurement_value>>;
      auto sep = dsl::sep(dsl::lit_c<','>);
      return dsl::list(item, sep);
      // return dsl::ascii::alnum + dsl::comma + dsl::list(item, sep);
    }();

    static constexpr auto value = lexy::as_list<std::vector<InfluxKV>>;
  };

  struct timestamp {
    static constexpr auto rule = dsl::integer<std::uint64_t>;
    static constexpr auto value = lexy::as_integer<std::uint64_t>;
  };

  // TODO this is only used for files, investigate how this works with UDP buffers
  struct InfluxMessage {
    static constexpr auto rule = [] {
      auto item = dsl::p<name> + dsl::lit_c<','> + dsl::p<tags> + dsl::lit_c<' '> + dsl::p<measurements>
                  + dsl::lit_c<' '> + dsl::p<timestamp> + dsl::eol;
      auto terminator = dsl::terminator(dsl::eof);
      return terminator.list(item);
    }();

    static constexpr auto value = lexy::as_list<std::vector<IMessage>>;
  };
} // namespace grammar
