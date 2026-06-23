#!/usr/bin/env bash
#
# Build and run the REST catalog unit tests (target: rest_catalog_test, which
# includes rest_json_serde_test.cc and rest_file_io_test.cc).
#
# Fast path: uses Homebrew LLVM (C++23 + libc++) and Homebrew apache-arrow, so
# Arrow is found via find_package instead of being compiled from source.
#
# One-time install:
#   brew install cmake ninja llvm apache-arrow
#
set -euo pipefail
cd "$(dirname "$0")"

LLVM_PREFIX="$(brew --prefix llvm)"
export SDKROOT="${SDKROOT:-$(xcrun --show-sdk-path)}"   # help LLVM clang find the macOS SDK

# NOTE: RelWithDebInfo (not Debug) on purpose. The vendored nanoarrow applies
# `-Werror -Wpedantic` only in Debug ($<$<CONFIG:Debug>:...>), and Homebrew's
# Clang 22 flags nanoarrow's use of `__COUNTER__` as a C2y extension, which then
# becomes a fatal error. Non-Debug configs drop those flags.
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_COMPILER="${LLVM_PREFIX}/bin/clang++" \
  -DCMAKE_C_COMPILER="${LLVM_PREFIX}/bin/clang" \
  -DCMAKE_PREFIX_PATH="$(brew --prefix apache-arrow);$(brew --prefix)" \
  -DICEBERG_BUILD_REST=ON \
  -DICEBERG_BUILD_TESTS=ON
  # If the Homebrew Arrow version mismatches and fails to compile, force the
  # vendored Arrow 24.0.0 build instead by adding:
  #   -DFETCHCONTENT_TRY_FIND_PACKAGE_MODE=NEVER

cmake --build build --target rest_catalog_test -j

ctest --test-dir build -R rest_catalog_test --output-on-failure
