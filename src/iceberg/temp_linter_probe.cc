// TEMP verification file — will be dropped before merge.
// Confirms cpp-linter runs clang-tidy with the pip-installed binary
// (via --version=${pythonLocation}/bin) after dropping clang-format.
namespace iceberg {

class TempLinterProbe {
 public:
  int* probe() {
    int* ptr = 0;  // modernize-use-nullptr should flag this
    return ptr;
  }

 private:
  int badMember;  // readability-identifier-naming: missing trailing underscore
};

}  // namespace iceberg
