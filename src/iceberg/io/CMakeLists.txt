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

if(NOT ICEBERG_IO)
  return()
endif()

set(ICEBERG_IO_SOURCES fs_file_io.cc fs_file_reader.cc fs_file_writer.cc)
set(ICEBERG_IO_INCLUDES "${ICEBERG_INCLUDES}")

add_iceberg_lib(iceberg_io
                SOURCES
                ${ICEBERG_IO_SOURCES}
                PRIVATE_INCLUDES
                ${ICEBERG_IO_INCLUDES})

iceberg_install_all_headers(iceberg/io)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/iceberg_io_export.h
        DESTINATION ${ICEBERG_INSTALL_INCLUDEDIR}/iceberg/io)
