#include <lexy/callback.hpp>
#include <lexy/dsl.hpp>
#include <string>
#include <vector>


struct InfluxKV {
  std::string name;
  std::string value;
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
    static constexpr auto rule = [] {
      auto head = dsl::ascii::digit;
      auto tail = dsl::ascii::alpha_digit;
      return dsl::identifier(head, tail);
    }();

    // static constexpr auto value = lexy::as_integer<int>;
    static constexpr auto value = lexy::as_string<std::string>;
  };

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
} // namespace grammer
