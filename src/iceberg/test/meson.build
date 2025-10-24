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

gmock_main_dep = dependency('gmock_main')

conf_data = configuration_data()
conf_data.set('ICEBERG_TEST_RESOURCES', meson.current_source_dir() / 'resources')
configure_file(
    input: 'test_config.h.in',
    output: 'test_config.h',
    configuration: conf_data,
    install: true,
    install_dir: get_option('includedir') / 'iceberg/test',
)

iceberg_tests = {
    'schema_test': {
        'sources': files(
            'name_mapping_test.cc',
            'partition_field_test.cc',
            'partition_spec_test.cc',
            'schema_field_test.cc',
            'schema_test.cc',
            'schema_util_test.cc',
            'snapshot_test.cc',
            'sort_field_test.cc',
            'sort_order_test.cc',
            'transform_test.cc',
            'type_test.cc',
        ),
    },
    'table_test': {
        'sources': files(
            'json_internal_test.cc',
            'schema_json_test.cc',
            'table_metadata_builder_test.cc',
            'table_test.cc',
            'test_common.cc',
        ),
    },
    'expression_test': {
        'sources': files(
            'expression_test.cc',
            'literal_test.cc',
            'predicate_test.cc',
        ),
    },
    'json_serde_test': {
        'sources': files(
            'json_internal_test.cc',
            'metadata_serde_test.cc',
            'schema_json_test.cc',
            'test_common.cc',
        ),
    },
    'util_test': {
        'sources': files(
            'bucket_util_test.cc',
            'config_test.cc',
            'decimal_test.cc',
            'endian_test.cc',
            'formatter_test.cc',
            'string_util_test.cc',
            'truncate_util_test.cc',
            'uuid_test.cc',
            'visit_type_test.cc',
        ),
    },
    'roaring_test': {'sources': files('roaring_test.cc')},
}

if get_option('rest').enabled()
    cpp_httplib_dep = dependency('cpp-httplib')
    iceberg_tests += {
        'rest_catalog_test': {
            'sources': files('rest_catalog_test.cc'),
            'dependencies': [iceberg_rest_dep, cpp_httplib_dep],
        },
    }
endif

foreach test_name, values : iceberg_tests
    exc = executable(
        test_name,
        sources: values['sources'],
        dependencies: [iceberg_dep, gmock_main_dep] + values.get(
            'dependencies',
            [],
        ),
    )
    test(test_name, exc)
endforeach
