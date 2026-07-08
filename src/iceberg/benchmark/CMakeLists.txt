# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(BENCHMARK_ENABLE_GTEST_TESTS
    OFF
    CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL
    OFF
    CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_TESTING
    OFF
    CACHE BOOL "" FORCE)

fetchcontent_declare(google_benchmark
                     GIT_REPOSITORY https://github.com/google/benchmark.git
                     GIT_TAG a4cf155615c63e019ae549e31703bf367df5b471 # v1.8.4
                     FIND_PACKAGE_ARGS
                     NAMES
                     benchmark
                     CONFIG)
fetchcontent_makeavailable(google_benchmark)

add_executable(benchmark_smoke benchmark_smoke.cc)
target_link_libraries(benchmark_smoke
                      PRIVATE "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>"
                              "$<IF:$<TARGET_EXISTS:benchmark::benchmark>,benchmark::benchmark,benchmark>"
)
