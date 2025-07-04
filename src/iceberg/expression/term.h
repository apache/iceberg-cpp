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

/// \file iceberg/expression/term.h

#include <string>

#include "iceberg/result.h"
#include "iceberg/schema.h"

namespace iceberg {

struct BoundedReference {};

/// Unbounded reference type for expressions.
struct Reference {
  using BoundedType = BoundedReference;

  std::string name;

  std::string ToString() const { return "Reference(name: " + name + ")"; }

  Result<BoundedReference> Bind(const Schema& schema, bool case_sensitive) const;
};

using Term = Reference;

}  // namespace iceberg