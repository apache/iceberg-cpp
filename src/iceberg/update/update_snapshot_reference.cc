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

#include "iceberg/update/update_snapshot_reference.h"

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg {

Result<std::shared_ptr<UpdateSnapshotReference>> UpdateSnapshotReference::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create UpdateSnapshotReference without a transaction");
  return std::shared_ptr<UpdateSnapshotReference>(
      new UpdateSnapshotReference(std::move(transaction)));
}

UpdateSnapshotReference::UpdateSnapshotReference(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {
  // Initialize updated_refs_ with current refs from base metadata
  const auto& base_refs = transaction_->current().refs;
  for (const auto& [name, ref] : base_refs) {
    updated_refs_[name] = ref;
  }
}

UpdateSnapshotReference::~UpdateSnapshotReference() = default;

UpdateSnapshotReference& UpdateSnapshotReference::CreateBranch(const std::string& name,
                                                               int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Branch name cannot be empty");
  // TODO(anyone): use MakeBranch after #408 got merged
  auto branch = std::make_shared<SnapshotRef>(
      SnapshotRef{.snapshot_id = snapshot_id, .retention = SnapshotRef::Branch{}});
  auto [_, inserted] = updated_refs_.emplace(name, std::move(branch));
  ICEBERG_BUILDER_CHECK(inserted, "Ref '{}' already exists", name);
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::CreateTag(const std::string& name,
                                                            int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Tag name cannot be empty");
  auto tag = std::make_shared<SnapshotRef>(
      SnapshotRef{.snapshot_id = snapshot_id, .retention = SnapshotRef::Tag{}});
  auto [_, inserted] = updated_refs_.emplace(name, std::move(tag));
  ICEBERG_BUILDER_CHECK(inserted, "Ref '{}' already exists", name);
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::RemoveBranch(const std::string& name) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Branch name cannot be empty");
  ICEBERG_BUILDER_CHECK(name != SnapshotRef::kMainBranch, "Cannot remove main branch");
  auto it = updated_refs_.find(name);
  ICEBERG_BUILDER_CHECK(it != updated_refs_.end(), "Branch does not exist: {}", name);
  ICEBERG_BUILDER_CHECK(it->second->type() == SnapshotRefType::kBranch,
                        "Ref '{}' is a tag not a branch", name);
  updated_refs_.erase(it);
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::RemoveTag(const std::string& name) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Tag name cannot be empty");
  auto it = updated_refs_.find(name);
  ICEBERG_BUILDER_CHECK(it != updated_refs_.end(), "Tag does not exist: {}", name);
  ICEBERG_BUILDER_CHECK(it->second->type() == SnapshotRefType::kTag,
                        "Ref '{}' is a branch not a tag", name);
  updated_refs_.erase(it);
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::RenameBranch(
    const std::string& name, const std::string& new_name) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Branch to rename cannot be empty");
  ICEBERG_BUILDER_CHECK(!new_name.empty(), "New branch name cannot be empty");
  ICEBERG_BUILDER_CHECK(name != SnapshotRef::kMainBranch, "Cannot rename main branch");
  auto it = updated_refs_.find(name);
  ICEBERG_BUILDER_CHECK(it != updated_refs_.end(), "Branch does not exist: {}", name);
  ICEBERG_BUILDER_CHECK(it->second->type() == SnapshotRefType::kBranch,
                        "Ref '{}' is a tag not a branch", name);
  auto [_, inserted] = updated_refs_.emplace(new_name, it->second);
  ICEBERG_BUILDER_CHECK(inserted, "Ref '{}' already exists", new_name);
  updated_refs_.erase(it);
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::ReplaceBranch(const std::string& name,
                                                                int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Branch name cannot be empty");
  auto it = updated_refs_.find(name);
  ICEBERG_BUILDER_CHECK(it != updated_refs_.end(), "Branch does not exist: {}", name);
  ICEBERG_BUILDER_CHECK(it->second->type() == SnapshotRefType::kBranch,
                        "Ref '{}' is a tag not a branch", name);
  it->second->snapshot_id = snapshot_id;
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::ReplaceBranch(const std::string& from,
                                                                const std::string& to) {
  return ReplaceBranchInternal(from, to, false);
}

UpdateSnapshotReference& UpdateSnapshotReference::FastForward(const std::string& from,
                                                              const std::string& to) {
  return ReplaceBranchInternal(from, to, true);
}

UpdateSnapshotReference& UpdateSnapshotReference::ReplaceBranchInternal(
    const std::string& from, const std::string& to, bool fast_forward) {
  ICEBERG_BUILDER_CHECK(!from.empty(), "Branch to update cannot be empty");
  ICEBERG_BUILDER_CHECK(!to.empty(), "Destination ref cannot be empty");
  auto to_it = updated_refs_.find(to);
  ICEBERG_BUILDER_CHECK(to_it != updated_refs_.end(), "Ref does not exist: {}", to);

  auto from_it = updated_refs_.find(from);
  if (from_it == updated_refs_.end()) {
    // Create branch if it doesn't exist
    return CreateBranch(from, to_it->second->snapshot_id);
  }

  ICEBERG_BUILDER_CHECK(from_it->second->type() == SnapshotRefType::kBranch,
                        "Ref '{}' is a tag not a branch", from);

  // Nothing to replace if snapshot IDs are the same
  if (to_it->second->snapshot_id == from_it->second->snapshot_id) {
    return *this;
  }

  if (fast_forward) {
    // Validate that current branch snapshot (from) is an ancestor of target snapshot (to)
    // This ensures we can fast-forward from 'from' to 'to'
    // TODO(anyone): use base() after #408
    const auto& base_metadata = transaction_->current();
    auto lookup = [&base_metadata](int64_t id) -> Result<std::shared_ptr<Snapshot>> {
      return base_metadata.SnapshotById(id);
    };
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(
        bool target_is_ancestor,
        SnapshotUtil::IsAncestorOf(to_it->second->snapshot_id,
                                   from_it->second->snapshot_id, lookup));
    ICEBERG_BUILDER_CHECK(target_is_ancestor,
                          "Cannot fast-forward: {} is not an ancestor of {}", from, to);
  }

  from_it->second->snapshot_id = to_it->second->snapshot_id;
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::ReplaceTag(const std::string& name,
                                                             int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Tag name cannot be empty");
  auto it = updated_refs_.find(name);
  ICEBERG_BUILDER_CHECK(it != updated_refs_.end(), "Tag does not exist: {}", name);
  ICEBERG_BUILDER_CHECK(it->second->type() == SnapshotRefType::kTag,
                        "Ref '{}' is a branch not a tag", name);
  it->second->snapshot_id = snapshot_id;
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::SetMinSnapshotsToKeep(
    const std::string& name, int32_t min_snapshots_to_keep) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Branch name cannot be empty");
  auto it = updated_refs_.find(name);
  ICEBERG_BUILDER_CHECK(it != updated_refs_.end(), "Branch does not exist: {}", name);
  ICEBERG_BUILDER_CHECK(it->second->type() == SnapshotRefType::kBranch,
                        "Ref '{}' is a tag not a branch", name);
  // TODO(anyone): use interface added in #408
  std::get<SnapshotRef::Branch>(it->second->retention).min_snapshots_to_keep =
      min_snapshots_to_keep;
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::SetMaxSnapshotAgeMs(
    const std::string& name, int64_t max_snapshot_age_ms) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Branch name cannot be empty");
  auto it = updated_refs_.find(name);
  ICEBERG_BUILDER_CHECK(it != updated_refs_.end(), "Branch does not exist: {}", name);
  ICEBERG_BUILDER_CHECK(it->second->type() == SnapshotRefType::kBranch,
                        "Ref '{}' is a tag not a branch", name);
  std::get<SnapshotRef::Branch>(it->second->retention).max_snapshot_age_ms =
      max_snapshot_age_ms;
  return *this;
}

UpdateSnapshotReference& UpdateSnapshotReference::SetMaxRefAgeMs(const std::string& name,
                                                                 int64_t max_ref_age_ms) {
  ICEBERG_BUILDER_CHECK(!name.empty(), "Reference name cannot be empty");
  auto it = updated_refs_.find(name);
  ICEBERG_BUILDER_CHECK(it != updated_refs_.end(), "Ref does not exist: {}", name);
  if (it->second->type() == SnapshotRefType::kBranch) {
    std::get<SnapshotRef::Branch>(it->second->retention).max_ref_age_ms = max_ref_age_ms;
  } else {
    std::get<SnapshotRef::Tag>(it->second->retention).max_ref_age_ms = max_ref_age_ms;
  }
  return *this;
}

Result<std::unordered_map<std::string, std::shared_ptr<SnapshotRef>>>
UpdateSnapshotReference::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  return updated_refs_;
}

}  // namespace iceberg
