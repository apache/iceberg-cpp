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

iceberg_rest_sources = files(
    'auth/auth_manager.cc',
    'auth/auth_managers.cc',
    'auth/auth_properties.cc',
    'auth/auth_session.cc',
    'auth/oauth2_util.cc',
    'auth/token_refresh_scheduler.cc',
    'catalog_properties.cc',
    'endpoint.cc',
    'error_handlers.cc',
    'http_client.cc',
    'json_serde.cc',
    'resource_paths.cc',
    'rest_catalog.cc',
    'rest_file_io.cc',
    'rest_metrics_reporter.cc',
    'rest_util.cc',
    'types.cc',
)
# CMake vendors cpr statically, including when Iceberg builds shared libraries.
cpr_dep = dependency(
    'cpr',
    static: true,
    default_options: ['default_library=static'],
)

iceberg_rest_sources += files('auth/sigv4_manager.cc')
iceberg_rest_build_deps = [cpr_dep]
iceberg_rest_compile_defs = []

sigv4_opt = get_option('sigv4')
# Use the CMake config, not pkg-config: aws-cpp-sdk-core.pc Cflags force
# -std=c++11 -fno-exceptions, which would override the project's C++23 build.
aws_sdk_core_dep = dependency(
    'aws-cpp-sdk-core',
    method: 'cmake',
    modules: ['aws-cpp-sdk-core'],
    required: sigv4_opt,
)
if aws_sdk_core_dep.found()
    iceberg_rest_build_deps += aws_sdk_core_dep
    iceberg_rest_compile_defs += '-DICEBERG_SIGV4_ENABLED=1'
else
    iceberg_rest_compile_defs += '-DICEBERG_SIGV4_ENABLED=0'
endif

iceberg_libraries += {
    'iceberg_rest': {
        'sources': iceberg_rest_sources,
        'dependencies': iceberg_rest_build_deps,
        'parent': 'iceberg',
        'compile_args': iceberg_rest_compile_defs,
    },
}

install_headers(
    [
        'catalog_properties.h',
        'constant.h',
        'endpoint.h',
        'error_handlers.h',
        'http_client.h',
        'http_request.h',
        'iceberg_rest_export.h',
        'resource_paths.h',
        'rest_catalog.h',
        'rest_file_io.h',
        'rest_util.h',
        'type_fwd.h',
        'types.h',
    ],
    subdir: 'iceberg/catalog/rest',
)

subdir('auth')
