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

/// \file iceberg/util/visit_type.h
/// \brief Visitor pattern for Iceberg types
/// Adapted from Apache Arrow
/// https://github.com/apache/arrow/blob/main/cpp/src/arrow/visit_type_inline.h

#include <utility>

#include "iceberg/result.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/visitor_generate.h"

namespace iceberg {

#define TYPE_VISIT_INLINE(TYPE_CLASS)                                   \
  case TYPE_CLASS##Type::kTypeId:                                       \
    return visitor->Visit(                                              \
        iceberg::internal::checked_cast<const TYPE_CLASS##Type&>(type), \
        std::forward<ARGS>(args)...);

/// \brief Calls `visitor` with the corresponding concrete type class
///
/// \tparam VISITOR Visitor type that implements Visit() for all Iceberg types.
/// \tparam ARGS Additional arguments, if any, will be passed to the Visit function after
/// the `type` argument
/// \return Status
///
/// A visitor is a type that implements specialized logic for each Iceberg type.
/// Example usage:
///
/// ```
/// class ExampleVisitor {
///   Status Visit(const IntType& type) { ... }
///   Status Visit(const LongType& type) { ... }
///   ...
/// }
/// ExampleVisitor visitor;
/// VisitTypeInline(some_type, &visitor);
/// ```

template <typename VISITOR, typename... ARGS>
inline Status VisitTypeInline(const Type& type, VISITOR* visitor, ARGS&&... args) {
  switch (type.type_id()) {
    ICEBERG_GENERATE_FOR_ALL_TYPES(TYPE_VISIT_INLINE);
    default:
      break;
  }
  return NotImplemented("Type not implemented");
}

#undef TYPE_VISIT_INLINE

#define TYPE_VISIT_INLINE(TYPE_CLASS)                          \
  case TYPE_CLASS##Type::kTypeId:                              \
    return std::forward<VISITOR>(visitor)(                     \
        internal::checked_cast<const TYPE_CLASS##Type&>(type), \
        std::forward<ARGS>(args)...);

/// \brief Call `visitor` with the corresponding concrete type class
/// \tparam ARGS Additional arguments, if any, will be passed to the Visit function after
/// the `type` argument
///
/// Unlike VisitTypeInline which calls `visitor.Visit`, here `visitor`
/// itself is called.
/// `visitor` must support a `const Type&` argument as a fallback,
/// in addition to concrete type classes.
///
/// The intent is for this to be called on a generic lambda
/// that may internally use `if constexpr` or similar constructs.
template <typename VISITOR, typename... ARGS>
inline auto VisitType(const Type& type, VISITOR&& visitor, ARGS&&... args)
    -> decltype(std::forward<VISITOR>(visitor)(type, args...)) {
  switch (type.type_id()) {
    ICEBERG_GENERATE_FOR_ALL_TYPES(TYPE_VISIT_INLINE);
    default:
      std::unreachable();
  }
}

#undef TYPE_VISIT_INLINE

#define TYPE_ID_VISIT_INLINE(TYPE_CLASS)                              \
  case TYPE_CLASS##Type::kTypeId: {                                   \
    const TYPE_CLASS##Type* concrete_ptr = nullptr;                   \
    return visitor->Visit(concrete_ptr, std::forward<ARGS>(args)...); \
  }

/// \brief Calls `visitor` with a nullptr of the corresponding concrete type class
///
/// \tparam VISITOR Visitor type that implements Visit() for all Iceberg types.
/// \tparam ARGS Additional arguments, if any, will be passed to the Visit function after
/// the `type` argument
/// \return Status
template <typename VISITOR, typename... ARGS>
inline Status VisitTypeIdInline(TypeId id, VISITOR* visitor, ARGS&&... args) {
  switch (id) {
    ICEBERG_GENERATE_FOR_ALL_TYPES(TYPE_ID_VISIT_INLINE);
    default:
      break;
  }
  return NotImplemented("Type not implemented");
}

#undef TYPE_ID_VISIT_INLINE

/// \brief Visit a type using a categorical visitor pattern
///
/// This function provides a simplified visitor interface that groups Iceberg types into
/// four categories based on their structural properties:
///
/// - **Struct types**: Complex types with named fields (StructType)
/// - **List types**: Sequential container types (ListType)
/// - **Map types**: Key-value container types (MapType)
/// - **Primitive types**: All leaf types without nested structure (14 primitive types)
///
/// This grouping is useful for algorithms that need to distinguish between container
/// types and leaf types, but don't require separate handling for each primitive type
/// variant (e.g., Int vs Long vs String).
///
/// \tparam VISITOR Visitor class that must implement four Visit methods:
///   - `VisitStruct(const StructType&, ARGS...)` for struct types
///   - `VisitList(const ListType&, ARGS...)` for list types
///   - `VisitMap(const MapType&, ARGS...)` for map types
///   - `VisitPrimitive(const PrimitiveType&, ARGS...)` for all primitive types
/// \tparam ARGS Additional argument types forwarded to Visit methods
/// \param type The type to visit
/// \param visitor Pointer to the visitor instance
/// \param args Additional arguments forwarded to the Visit methods
/// \return The return value from the invoked Visit method
template <typename VISITOR, typename... ARGS>
inline auto VisitTypeCategory(const Type& type, VISITOR* visitor, ARGS&&... args) {
#define SCHEMA_VISIT_ACTION(TYPE_CLASS)                      \
  return visitor->Visit##TYPE_CLASS(                         \
      internal::checked_cast<const TYPE_CLASS##Type&>(type), \
      std::forward<ARGS>(args)...);

  switch (type.type_id()) {
    ICEBERG_TYPE_SWITCH_WITH_PRIMITIVE_DEFAULT(SCHEMA_VISIT_ACTION)
  }

#undef SCHEMA_VISIT_ACTION
}

}  // namespace iceberg
