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

# Match ICEBERG_HIVE_SOURCES and ICEBERG_HIVE_THRIFT_GEN_SOURCES.
if get_option('bundle_thrift')
    if not bundle_enabled
        error('bundle_thrift requires bundle=enabled')
    endif
    if arrow_dep.type_name() != 'internal'
        error(
            'bundle_thrift requires the Arrow source fallback; use --force-fallback-for=arrow or -Dbundle_thrift=false',
        )
    endif
    hive_thrift_dep = arrow_thrift_dep
else
    hive_thrift_dep = dependency('thrift', required: true, allow_fallback: false)
endif

iceberg_hive_sources = files(
    'gen-cpp/FacebookService.cpp',
    'gen-cpp/ThriftHiveMetastore.cpp',
    'gen-cpp/fb303_types.cpp',
    'gen-cpp/hive_metastore_constants.cpp',
    'gen-cpp/hive_metastore_types.cpp',
    'hive_catalog.cc',
    'hive_catalog_properties.cc',
    'hms_client.cc',
)
iceberg_libraries += {
    'iceberg_hive': {
        'sources': iceberg_hive_sources,
        'dependencies': [hive_thrift_dep],
        'parent': 'iceberg',
        'include_directories': [include_directories('gen-cpp')],
        'compile_args': cpp.get_supported_arguments(
            '-Wno-error=deprecated-declarations',
        ),
    },
}
install_headers(
    [
        'hive_catalog.h',
        'hive_catalog_properties.h',
        'hms_client.h',
        'iceberg_hive_export.h',
    ],
    subdir: 'iceberg/catalog/hive',
)
