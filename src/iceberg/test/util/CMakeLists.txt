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

set(ICEBERG_TEST_UTIL_SOURCES common_util.cc)
set(ICEBERG_TEST_UTIL_HEADERS common_util.h)

# Platform-specific integration test utilities (Linux-specific)
if(ICEBERG_BUILD_REST_INTEGRATION_TESTS)
  list(APPEND ICEBERG_TEST_UTIL_SOURCES cmd_util.cc docker_compose_util.cc)
  list(APPEND ICEBERG_TEST_UTIL_HEADERS cmd_util.h docker_compose_util.h)
endif()

add_library(iceberg_test_util STATIC ${ICEBERG_TEST_UTIL_SOURCES})

target_include_directories(iceberg_test_util PUBLIC ${CMAKE_SOURCE_DIR}/src
                                                    ${CMAKE_BINARY_DIR}/src)

target_link_libraries(iceberg_test_util PUBLIC iceberg_static GTest::gtest
                                               nlohmann_json::nlohmann_json)

install(FILES ${ICEBERG_TEST_UTIL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/iceberg/test/util)
