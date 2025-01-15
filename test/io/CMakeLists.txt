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

if(ICEBERG_IO)
  add_executable(fs_file_io_test)
  target_sources(fs_file_io_test PRIVATE fs_file_io_test.cc)
  target_link_libraries(fs_file_io_test PRIVATE iceberg_io_static GTest::gtest_main)
  target_include_directories(fs_file_io_test PRIVATE "${ICEBERG_INCLUDES}")
  add_test(NAME fs_file_io_test COMMAND fs_file_io_test)
endif()
