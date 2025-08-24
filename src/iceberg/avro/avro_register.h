/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

/// \file iceberg/avro/avro_register.h
/// \brief Provide functions to register Avro implementations.

#include "iceberg/iceberg_bundle_export.h"

namespace iceberg::avro {

/// \brief Register all the logical types.
ICEBERG_BUNDLE_EXPORT void RegisterLogicalTypes();

/// \brief Register Avro reader implementation.
ICEBERG_BUNDLE_EXPORT void RegisterReader();

/// \brief Register Avro writer implementation.
ICEBERG_BUNDLE_EXPORT void RegisterWriter();

/// \brief Register all the logical types, Avro reader, and Avro writer.
ICEBERG_BUNDLE_EXPORT void RegisterAll();

}  // namespace iceberg::avro
