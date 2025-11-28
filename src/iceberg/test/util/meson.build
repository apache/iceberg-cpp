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

iceberg_test_util_sources = files('common_util.cc')

# Platform-specific integration test utilities (Linux-specific)
if get_option('rest_integration_test').enabled()
    iceberg_test_util_sources += files('cmd_util.cc', 'docker_compose_util.cc')
    install_headers(
        ['cmd_util.h', 'docker_compose_util.h'],
        subdir: 'iceberg/test/util',
    )
endif

gtest_dep = dependency('gtest')
nlohmann_json_dep = dependency('nlohmann_json')

iceberg_test_util_lib = static_library(
    'iceberg_test_util',
    sources: iceberg_test_util_sources,
    dependencies: [
        iceberg_dep,
        gtest_dep.partial_dependency(compile_args: true, includes: true),
        nlohmann_json_dep,
    ],
    include_directories: include_directories('../..'),
)

iceberg_test_util_dep = declare_dependency(
    link_with: iceberg_test_util_lib,
    include_directories: include_directories('.'),
)

install_headers(['common_util.h'], subdir: 'iceberg/test/util')
