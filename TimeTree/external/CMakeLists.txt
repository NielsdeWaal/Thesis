include(FetchContent)
FetchContent_Declare(
    fmt

    GIT_REPOSITORY git@github.com:fmtlib/fmt.git
    GIT_TAG 9.1.0
)
FetchContent_Declare(
    tl-expected

    GIT_REPOSITORY git@github.com:TartanLlama/expected.git
    GIT_TAG v1.1.0
)

FetchContent_MakeAvailable(fmt tl-expected)
