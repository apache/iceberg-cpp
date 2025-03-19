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

if(ICEBERG_ARROW)
  add_executable(arrow_unittest)
  target_sources(arrow_unittest PRIVATE arrow_test.cc)
  target_link_libraries(arrow_unittest PRIVATE iceberg_arrow_static Arrow::arrow_static
                                               GTest::gtest_main)
  target_include_directories(arrow_unittest PRIVATE "${ICEBERG_INCLUDES}")
  add_test(NAME arrow_unittest COMMAND arrow_unittest)
endif()
