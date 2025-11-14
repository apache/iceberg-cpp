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

#include "iceberg/expression/rewrite_not.h"

namespace iceberg {

Result<std::shared_ptr<Expression>> RewriteNot::Visit(std::shared_ptr<Expression> expr) {
  ICEBERG_DCHECK(expr != nullptr, "Expression cannot be null");
  RewriteNot visitor;
  return iceberg::Visit<std::shared_ptr<Expression>, RewriteNot>(expr, visitor);
}

Result<std::shared_ptr<Expression>> RewriteNot::AlwaysTrue() { return True::Instance(); }

Result<std::shared_ptr<Expression>> RewriteNot::AlwaysFalse() {
  return False::Instance();
}

Result<std::shared_ptr<Expression>> RewriteNot::Not(
    const std::shared_ptr<Expression>& child_result) {
  return child_result->Negate();
}

Result<std::shared_ptr<Expression>> RewriteNot::And(
    const std::shared_ptr<Expression>& left_result,
    const std::shared_ptr<Expression>& right_result) {
  return And::MakeFolded(left_result, right_result);
}

Result<std::shared_ptr<Expression>> RewriteNot::Or(
    const std::shared_ptr<Expression>& left_result,
    const std::shared_ptr<Expression>& right_result) {
  return Or::MakeFolded(left_result, right_result);
}

Result<std::shared_ptr<Expression>> RewriteNot::Predicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  return pred;
}

Result<std::shared_ptr<Expression>> RewriteNot::Predicate(
    const std::shared_ptr<UnboundPredicate>& pred) {
  return pred;
}

}  // namespace iceberg
