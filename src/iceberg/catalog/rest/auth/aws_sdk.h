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

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/aws_sdk.h
/// \brief Process-wide AWS SDK lifecycle for SigV4 authentication.
///
/// Applications using SigV4 should call InitializeAwsSdk() at startup and
/// FinalizeAwsSdk() before exit. If never called, the SDK is lazily
/// initialized on first SigV4 use and leaked at process exit. FinalizeAwsSdk()
/// is intended for process-shutdown sequencing, not concurrent teardown.

namespace iceberg::rest::auth {

/// \brief Initialize the AWS SDK. Idempotent.
ICEBERG_REST_EXPORT Status InitializeAwsSdk();

/// \brief Shut down the AWS SDK. Refuses if any SigV4 sessions are alive.
ICEBERG_REST_EXPORT Status FinalizeAwsSdk();

ICEBERG_REST_EXPORT bool IsAwsSdkInitialized();
ICEBERG_REST_EXPORT bool IsAwsSdkFinalized();

}  // namespace iceberg::rest::auth
