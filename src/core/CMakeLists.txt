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

set(ICEBERG_CORE_SOURCES demo_table.cc)
set(ICEBERG_CORE_INCLUDES "${ICEBERG_API_DIR}")

add_iceberg_lib(iceberg_core
                SOURCES
                ${ICEBERG_CORE_SOURCES}
                OUTPUTS
                ICEBERG_CORE_LIBRARIES
                PRIVATE_INCLUDES
                ${ICEBERG_CORE_INCLUDES})

if(ICEBERG_BUILD_STATIC AND WIN32)
  target_compile_definitions(iceberg_core_static PUBLIC ICEBERG_STATIC)
endif()

foreach(LIB_TARGET ${ICEBERG_CORE_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ICEBERG_EXPORTING)
endforeach()
