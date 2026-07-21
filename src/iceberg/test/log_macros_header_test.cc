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

#include "iceberg/logging/logger.h"

#ifdef ICEBERG_LOG_INFO
#  error "logger.h must not define logging macros; include log_macros.h instead"
#endif

#ifdef LOG_INFO
#  error "logger.h must not define short logging macros"
#endif

#include "iceberg/logging/log_macros.h"

#ifndef ICEBERG_LOG_INFO
#  error "log_macros.h must define Iceberg-prefixed logging macros"
#endif

#ifdef LOG_INFO
#  error "log_macros.h must not define short logging macros by default"
#endif

#include "iceberg/logging/short_log_macros.h"

#ifndef LOG_INFO
#  error "short_log_macros.h must define opt-in short logging macros"
#endif

#include <gtest/gtest.h>

namespace iceberg {

TEST(LogMacrosHeaderTest, MacroHeadersExposeOnlyTheRequestedNames) { SUCCEED(); }

}  // namespace iceberg
