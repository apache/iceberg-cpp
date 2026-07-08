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

/// \file iceberg/logging/short_log_macros.h
/// \brief Opt-in short aliases for Iceberg logging macros.

#include "iceberg/logging/log_macros.h"

#define LOG_TRACE(...) ICEBERG_LOG_TRACE(__VA_ARGS__)
#define LOG_DEBUG(...) ICEBERG_LOG_DEBUG(__VA_ARGS__)
#define LOG_INFO(...) ICEBERG_LOG_INFO(__VA_ARGS__)
#define LOG_WARN(...) ICEBERG_LOG_WARN(__VA_ARGS__)
#define LOG_ERROR(...) ICEBERG_LOG_ERROR(__VA_ARGS__)
#define LOG_CRITICAL(...) ICEBERG_LOG_CRITICAL(__VA_ARGS__)
#define LOG_FATAL(...) ICEBERG_LOG_FATAL(__VA_ARGS__)
