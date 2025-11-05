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

#include <string_view>

#include "iceberg/version.h"

/// \file iceberg/catalog/rest/constant.h
/// Constant values for Iceberg REST API.

namespace iceberg::rest {

constexpr std::string_view kHeaderContentType = "Content-Type";
constexpr std::string_view kHeaderAccept = "Accept";
constexpr std::string_view kHeaderXClientVersion = "X-Client-Version";
constexpr std::string_view kHeaderUserAgent = "User-Agent";

constexpr std::string_view kMimeTypeApplicationJson = "application/json";
constexpr std::string_view kClientVersion = "0.14.1";
constexpr std::string_view kUserAgentPrefix = "iceberg-cpp/";
constexpr std::string_view kUserAgent = "iceberg-cpp/" ICEBERG_VERSION_STRING;

constexpr std::string_view kPathV1 = "v1";

}  // namespace iceberg::rest
