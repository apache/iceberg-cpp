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

#include <string>

namespace iceberg::arrow {

/// \brief S3 configuration property keys for ArrowS3FileIO.
///
/// These constants define the property keys used to configure S3 access
/// via the Arrow filesystem integration, following the Iceberg spec for
/// S3 configuration properties.
struct S3Properties {
  /// AWS access key ID
  static constexpr const char* kAccessKeyId = "s3.access-key-id";
  /// AWS secret access key
  static constexpr const char* kSecretAccessKey = "s3.secret-access-key";
  /// AWS session token (for temporary credentials)
  static constexpr const char* kSessionToken = "s3.session-token";
  /// AWS region
  static constexpr const char* kRegion = "s3.region";
  /// Custom endpoint override (for MinIO, LocalStack, etc.)
  static constexpr const char* kEndpoint = "s3.endpoint";
  /// Whether to use path-style access (needed for MinIO)
  static constexpr const char* kPathStyleAccess = "s3.path-style-access";
  /// Whether SSL is enabled
  static constexpr const char* kSslEnabled = "s3.ssl.enabled";
  /// Connection timeout in milliseconds
  static constexpr const char* kConnectTimeoutMs = "s3.connect-timeout-ms";
  /// Socket timeout in milliseconds
  static constexpr const char* kSocketTimeoutMs = "s3.socket-timeout-ms";
};

}  // namespace iceberg::arrow
