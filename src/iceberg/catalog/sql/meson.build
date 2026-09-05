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

# Match ICEBERG_SQL_CATALOG_SOURCES and the optional built-in connectors.
iceberg_sql_catalog_sources = files('connection_uri.cc', 'sql_catalog.cc')
sql_deps = []
sql_config = configuration_data()
sql_connectors = {
    'sqlite': ['sqlite3', 'SQLITE3', 'catalog_store_sqlite3.cc'],
    'postgresql': ['libpq', 'POSTGRESQL', 'catalog_store_postgresql.cc'],
    'mysql': ['mysqlclient', 'MYSQL', 'catalog_store_mysql.cc'],
}
foreach connector, spec : sql_connectors
    opt = get_option('sql_' + connector)
    native_dep = dependency(spec[0], required: opt, allow_fallback: false)
    enabled = native_dep.found()
    set_variable('sql_' + connector + '_enabled', enabled)
    if enabled
        sql_config.set('BUILD_' + spec[1] + '_CONNECTOR', true)
        iceberg_sql_catalog_sources += files(spec[2])
        sql_deps += native_dep
    endif
endforeach
if sql_deps.length() > 0
    # sqlpp23 is a build-only header dependency. Only the native database
    # clients belong to the installed library's link interface.
    sqlpp23_dep = dependency('sqlpp23', static: true, allow_fallback: false)
    sql_deps += sqlpp23_dep
endif
configure_file(
    input: 'config.h.in',
    output: 'config.h',
    format: 'cmake',
    configuration: sql_config,
)
iceberg_libraries += {
    'iceberg_sql_catalog': {
        'sources': iceberg_sql_catalog_sources,
        'dependencies': sql_deps,
        'parent': 'iceberg',
    },
}
install_headers(
    ['catalog_store.h', 'iceberg_sql_catalog_export.h', 'sql_catalog.h'],
    subdir: 'iceberg/catalog/sql',
)
