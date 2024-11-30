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

add_library(iceberg demo.cc)

target_compile_features(iceberg PUBLIC cxx_std_20)

target_include_directories(
  iceberg
  INTERFACE $<INSTALL_INTERFACE:${ICEBERG_INSTALL_INCLUDEDIR}>
  PUBLIC $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

install(
  TARGETS iceberg
  EXPORT icebergTargets
  LIBRARY DESTINATION ${ICEBERG_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${ICEBERG_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${ICEBERG_INSTALL_BINDIR}
  INCLUDES
  DESTINATION ${ICEBERG_INSTALL_INCLUDEDIR})

install(
  EXPORT icebergTargets
  FILE icebergTargets.cmake
  NAMESPACE iceberg::
  DESTINATION ${ICEBERG_INSTALL_CMAKEDIR})

include(CMakePackageConfigHelpers)

write_basic_package_version_file(
  "icebergConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/icebergConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/icebergConfig.cmake"
  INSTALL_DESTINATION ${ICEBERG_INSTALL_CMAKEDIR})

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/icebergConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/icebergConfigVersion.cmake"
        DESTINATION ${ICEBERG_INSTALL_CMAKEDIR})
