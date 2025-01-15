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

# Libraries to link with exported libiceberg_io.{so,a}.
set(ICEBERG_IO_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_IO_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_IO_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_IO_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND ICEBERG_IO_STATIC_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>")
list(APPEND ICEBERG_IO_SHARED_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>")
list(APPEND
     ICEBERG_IO_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:Iceberg::iceberg_static>,Iceberg::iceberg_static,Iceberg::iceberg_shared>"
)
list(APPEND
     ICEBERG_IO_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:Iceberg::iceberg_shared>,Iceberg::iceberg_shared,Iceberg::iceberg_static>"
)

add_iceberg_lib(iceberg_io
                SOURCES
                ${ICEBERG_IO_SOURCES}
                PRIVATE_INCLUDES
                ${ICEBERG_IO_INCLUDES}
                SHARED_LINK_LIBS
                ${ICEBERG_IO_SHARED_BUILD_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ICEBERG_IO_STATIC_BUILD_INTERFACE_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ICEBERG_IO_STATIC_INSTALL_INTERFACE_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ICEBERG_IO_SHARED_INSTALL_INTERFACE_LIBS})

iceberg_install_all_headers(iceberg/io)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/iceberg_io_export.h
        DESTINATION ${ICEBERG_INSTALL_INCLUDEDIR}/iceberg/io)
