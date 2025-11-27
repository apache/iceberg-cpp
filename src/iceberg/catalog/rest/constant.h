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

#include "iceberg/version.h"

/// \file iceberg/catalog/rest/constant.h
/// Constant values for Iceberg REST API.

namespace iceberg::rest {

inline const std::string kHeaderContentType = "Content-Type";
inline const std::string kHeaderAccept = "Accept";
inline const std::string kHeaderXClientVersion = "X-Client-Version";
inline const std::string kHeaderUserAgent = "User-Agent";

inline const std::string kMimeTypeApplicationJson = "application/json";
inline const std::string kMimeTypeFormUrlEncoded = "application/x-www-form-urlencoded";
inline const std::string kUserAgentPrefix = "iceberg-cpp/";
inline const std::string kUserAgent = "iceberg-cpp/" ICEBERG_VERSION_STRING;

inline const std::string kQueryParamParent = "parent";
inline const std::string kQueryParamPageToken = "page_token";

}  // namespace iceberg::rest
