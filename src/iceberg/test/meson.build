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
)

# CMake's tests prefer static libraries because they also exercise internal
# helpers. Keep those symbols private in installed shared libraries. Reuse
# compiled objects on Unix; Windows needs sources compiled with *_STATIC so
# references between the test libraries do not use DLL imports.
foreach name, spec : iceberg_libraries
    deps = spec.get('dependencies', [])
    parent = spec.get('parent', '')
    if parent != ''
        deps = [get_variable(parent + '_test_dep')] + deps
    endif
    test_args = spec.get('compile_args', [])
    if host_machine.system() == 'windows'
        test_args += '-D' + name.to_upper() + '_STATIC'
    endif
    if get_option('default_library') != 'shared'
        lib = get_variable(name + '_static_lib')
    elif host_machine.system() == 'windows'
        lib = static_library(
            name + '_test',
            sources: spec['sources'],
            dependencies: deps,
            include_directories: [iceberg_include_dir] + spec.get(
                'include_directories',
                [],
            ),
            cpp_args: test_args,
        )
    else
        lib = static_library(
            name + '_test',
            objects: get_variable(name + '_shared_lib').extract_all_objects(
                recursive: true,
            ),
        )
    endif
    set_variable(
        name + '_test_dep',
        declare_dependency(
            link_with: lib,
            dependencies: deps,
            include_directories: iceberg_include_dir,
            compile_args: test_args,
        ),
    )
endforeach

iceberg_tests = {
    'schema_test': {
        'sources': files(
            'assign_id_visitor_test.cc',
            'name_mapping_test.cc',
            'partition_field_test.cc',
            'partition_spec_test.cc',
            'partition_value_test.cc',
            'schema_field_test.cc',
            'schema_test.cc',
            'schema_util_test.cc',
            'sort_field_test.cc',
            'sort_order_test.cc',
            'transform_human_string_test.cc',
            'transform_test.cc',
            'type_test.cc',
        ),
    },
    'table_test': {
        'sources': files(
            'catalog_util_test.cc',
            'location_provider_test.cc',
            'metrics_config_test.cc',
            'metrics_reporter_test.cc',
            'metrics_test.cc',
            'snapshot_summary_builder_test.cc',
            'snapshot_test.cc',
            'snapshot_util_test.cc',
            'table_metadata_builder_test.cc',
            'table_requirement_test.cc',
            'table_requirements_test.cc',
            'table_test.cc',
            'table_update_test.cc',
        ),
    },
    'logging_test': {
        'sources': files(
            'cerr_logger_test.cc',
            'log_level_test.cc',
            'logger_test.cc',
            'loggers_test.cc',
            'macros_active_level_test.cc',
            'macros_test.cc',
            'spdlog_logger_test.cc',
        ),
    },
    'expression_test': {
        'sources': files(
            'aggregate_test.cc',
            'expression_json_test.cc',
            'expression_test.cc',
            'expression_visitor_test.cc',
            'inclusive_metrics_evaluator_test.cc',
            'inclusive_metrics_evaluator_with_transform_test.cc',
            'literal_test.cc',
            'manifest_evaluator_test.cc',
            'predicate_test.cc',
            'projections_test.cc',
            'residual_evaluator_test.cc',
            'strict_metrics_evaluator_test.cc',
        ),
    },
    'json_serde_test': {
        'sources': files(
            'json_serde_test.cc',
            'metadata_serde_test.cc',
            'schema_json_test.cc',
        ),
    },
    'util_test': {
        'sources': files(
            'base64_test.cc',
            'bucket_util_test.cc',
            'cache_test.cc',
            'config_test.cc',
            'content_file_util_test.cc',
            'data_file_set_test.cc',
            'decimal_test.cc',
            'endian_test.cc',
            'executor_util_test.cc',
            'file_io_test.cc',
            'formatter_test.cc',
            'iterator_test.cc',
            'lazy_test.cc',
            'location_util_test.cc',
            'math_util_internal_test.cc',
            'position_delete_index_test.cc',
            'position_delete_range_consumer_test.cc',
            'resolving_file_io_test.cc',
            'retry_util_test.cc',
            'roaring_position_bitmap_test.cc',
            'string_util_test.cc',
            'struct_like_set_test.cc',
            'task_group_test.cc',
            'temporal_util_test.cc',
            'transform_util_test.cc',
            'truncate_util_test.cc',
            'url_encoder_test.cc',
            'uuid_test.cc',
            'visit_type_test.cc',
        ),
        'use_data': true,
    },
    'puffin_test': {
        'sources': files(
            'puffin_format_test.cc',
            'puffin_json_test.cc',
            'puffin_reader_writer_test.cc',
        ),
        'use_data': true,
    },
}

if bundle_enabled
    iceberg_tests += {
        'avro_test': {
            'sources': files(
                'avro_data_test.cc',
                'avro_schema_test.cc',
                'avro_stream_test.cc',
                'avro_test.cc',
            ),
            'use_bundle': true,
        },
        'arrow_test': {
            'sources': files(
                'arrow_io_test.cc',
                'arrow_test.cc',
                'gzip_decompress_test.cc',
                'metadata_io_test.cc',
                'struct_like_test.cc',
            ),
            'use_bundle': true,
        },
        'catalog_test': {
            'sources': files('in_memory_catalog_test.cc'),
            'use_bundle': true,
        },
        'metadata_table_test': {
            'sources': files('metadata_table_test.cc'),
            'use_bundle': true,
        },
        'eval_expr_test': {
            'sources': files('eval_expr_test.cc', 'evaluator_test.cc'),
            'use_bundle': true,
        },
        'manifest_test': {
            'sources': files(
                'delete_file_index_test.cc',
                'manifest_group_test.cc',
                'manifest_list_versions_test.cc',
                'manifest_merge_manager_test.cc',
                'manifest_reader_stats_test.cc',
                'manifest_reader_test.cc',
                'manifest_writer_versions_test.cc',
                'rolling_manifest_writer_test.cc',
            ),
            'use_bundle': true,
        },
        'parquet_test': {
            'sources': files(
                'metrics_test_base.cc',
                'parquet_data_test.cc',
                'parquet_metrics_test.cc',
                'parquet_schema_test.cc',
                'parquet_test.cc',
            ),
            'use_bundle': true,
        },
        'scan_test': {
            'sources': files(
                'file_scan_task_test.cc',
                'incremental_append_scan_test.cc',
                'incremental_changelog_scan_test.cc',
                'scan_planning_metrics_test.cc',
                'table_scan_test.cc',
            ),
            'use_bundle': true,
        },
        'table_update_test': {
            'sources': files(
                'delete_files_test.cc',
                'expire_snapshots_test.cc',
                'fast_append_test.cc',
                'manifest_filter_manager_test.cc',
                'merge_append_test.cc',
                'merging_snapshot_update_test.cc',
                'name_mapping_update_test.cc',
                'overwrite_files_test.cc',
                'replace_partitions_test.cc',
                'rewrite_files_test.cc',
                'row_delta_test.cc',
                'snapshot_manager_test.cc',
                'transaction_test.cc',
                'update_location_test.cc',
                'update_partition_spec_test.cc',
                'update_partition_statistics_test.cc',
                'update_properties_test.cc',
                'update_schema_test.cc',
                'update_sort_order_test.cc',
                'update_statistics_test.cc',
            ),
            'use_bundle': true,
        },
        'data_test': {
            'sources': files(
                'arrow_c_data_util_test.cc',
                'arrow_row_builder_test.cc',
                'data_writer_test.cc',
                'default_value_test.cc',
                'delete_filter_test.cc',
                'delete_loader_test.cc',
                'dv_writer_test.cc',
                'file_scan_task_reader_test.cc',
                'literal_util_test.cc',
            ),
            'use_bundle': true,
        },
    }
endif

if get_option('rest').enabled()
    iceberg_tests += {
        'rest_catalog_test': {
            'sources': files(
                'auth_manager_test.cc',
                'endpoint_test.cc',
                'error_handlers_test.cc',
                'rest_file_io_test.cc',
                'rest_json_serde_test.cc',
                'rest_metrics_reporter_test.cc',
                'rest_util_test.cc',
            ),
            'dependencies': [iceberg_rest_test_dep],
        },
    }
    if bundle_enabled
        iceberg_tests += {
            'rest_arrow_file_io_test': {
                'sources': files('rest_arrow_file_io_test.cc'),
                'dependencies': [iceberg_rest_test_dep],
                'use_bundle': true,
            },
        }
    endif
    if aws_sdk_core_dep.found()
        iceberg_tests += {
            'sigv4_auth_test': {
                'sources': files('sigv4_auth_test.cc'),
                'dependencies': [iceberg_rest_test_dep, aws_sdk_core_dep],
            },
        }
    endif
    if get_option('rest_integration_test').enabled()
        if host_machine.system() == 'windows'
            warning('Cannot build rest integration test on Windows, skipping.')
        else
            iceberg_tests += {
                'rest_catalog_integration_test': {
                    'sources': files(
                        'rest_catalog_integration_test.cc',
                        'util/cmd_util.cc',
                        'util/docker_compose_util.cc',
                    ),
                    'dependencies': [iceberg_rest_test_dep],
                },
            }
        endif
    endif
endif

if s3_enabled
    iceberg_tests += {
        'file_io_test': {
            'sources': files('arrow_s3_file_io_test.cc'),
            'use_bundle': true,
        },
    }
endif
if get_option('hive').enabled()
    iceberg_tests += {
        'hive_catalog_test': {
            'sources': files('hms_client_test.cc'),
            'dependencies': [iceberg_hive_test_dep],
        },
    }
endif
if get_option('sql_catalog').enabled() and sql_sqlite_enabled and bundle_enabled
    iceberg_tests += {
        'sql_catalog_test': {
            'sources': files('sql_catalog_test.cc'),
            'dependencies': [iceberg_sql_catalog_test_dep],
            'use_bundle': true,
        },
    }
endif

foreach test_name, values : iceberg_tests
    test_deps = [gmock_main_dep] + values.get('dependencies', [])
    if values.get('use_bundle', false)
        test_deps = [iceberg_bundle_test_dep] + test_deps
    elif values.get('use_data', false)
        test_deps = [iceberg_data_test_dep] + test_deps
    else
        test_deps = [iceberg_test_dep] + test_deps
    endif

    exc = executable(
        test_name,
        sources: values['sources'],
        dependencies: test_deps,
    )
    test(test_name, exc)
endforeach
