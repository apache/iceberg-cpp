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

# Generate the .cc-only logging backend config header. The meson build always
# links spdlog, so ICEBERG_HAS_SPDLOG is always defined here. Generated into
# build/src/iceberg/logging/config.h (resolved via include_directories('..'),
# which exposes both the source and build trees); not installed.
logging_config_data = configuration_data()
logging_config_data.set('ICEBERG_HAS_SPDLOG', 1)
configure_file(output: 'config.h', configuration: logging_config_data)

# Public logging headers. The build-generated config.h and the internal
# SpdLogger header are intentionally NOT installed.
install_headers(
    [
        'cerr_logger.h',
        'log_level.h',
        'log_macros.h',
        'logger.h',
        'loggers.h',
        'short_log_macros.h',
    ],
    subdir: 'iceberg/logging',
)
