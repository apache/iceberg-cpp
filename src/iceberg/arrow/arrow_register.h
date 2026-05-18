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

/// \file iceberg/arrow/arrow_register.h
/// \brief Provide functions to register Arrow bundle integrations.

#include "iceberg/iceberg_bundle_export.h"

namespace iceberg::arrow {

/// \brief Register Arrow FileIOs and Arrow-backed C Data utilities.
///
/// This operation is idempotent and safe to call multiple times.
ICEBERG_BUNDLE_EXPORT void RegisterAll();

}  // namespace iceberg::arrow
