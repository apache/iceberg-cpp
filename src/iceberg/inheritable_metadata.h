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

/// \file iceberg/inheritable_metadata.h
/// Metadata inheritance system for manifest entries.

#include <cstdint>
#include <memory>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Interface for applying inheritable metadata to manifest entries.
///
/// When manifest entries have null values for certain fields (snapshot id,
/// data sequence number, file sequence number), these values should be inherited
/// from the manifest file. This interface provides a way to apply such inheritance rules.
class ICEBERG_EXPORT InheritableMetadata {
 public:
  virtual ~InheritableMetadata();

  /// \brief Apply inheritable metadata to a manifest entry.
  /// \param entry The manifest entry to modify.
  /// \return Status indicating success or failure.
  virtual Status Apply(ManifestEntry& entry) = 0;
};

/// \brief Base implementation of InheritableMetadata that handles standard inheritance
/// rules.
class ICEBERG_EXPORT BaseInheritableMetadata : public InheritableMetadata {
 public:
  /// \brief Constructor for base inheritable metadata.
  /// \param spec_id Partition spec ID from the manifest.
  /// \param snapshot_id Snapshot ID from the manifest.
  /// \param sequence_number Sequence number from the manifest.
  /// \param manifest_location Path to the manifest file.
  BaseInheritableMetadata(int32_t spec_id, int64_t snapshot_id, int64_t sequence_number,
                          std::string manifest_location);

  Status Apply(ManifestEntry& entry) override;

  ~BaseInheritableMetadata() override;

 private:
  int32_t spec_id_;
  int64_t snapshot_id_;
  int64_t sequence_number_;
  std::string manifest_location_;
};

/// \brief Empty implementation that applies no inheritance.
class ICEBERG_EXPORT EmptyInheritableMetadata : public InheritableMetadata {
 public:
  Status Apply(ManifestEntry& entry) override;

  ~EmptyInheritableMetadata() override;
};

/// \brief Metadata inheritance for copying manifests before commit.
class ICEBERG_EXPORT CopyInheritableMetadata : public InheritableMetadata {
 public:
  /// \brief Constructor for copy metadata.
  /// \param snapshot_id The snapshot ID to use for copying.
  explicit CopyInheritableMetadata(int64_t snapshot_id);

  Status Apply(ManifestEntry& entry) override;

  ~CopyInheritableMetadata() override;

 private:
  int64_t snapshot_id_;
};

/// \brief Factory for creating InheritableMetadata instances.
class ICEBERG_EXPORT InheritableMetadataFactory {
 public:
  /// \brief Create an empty metadata instance that applies no inheritance.
  static Result<std::unique_ptr<InheritableMetadata>> Empty();

  /// \brief Create metadata instance from a manifest file.
  /// \param manifest The manifest file to extract metadata from.
  /// \return Inheritable metadata based on the manifest.
  static Result<std::unique_ptr<InheritableMetadata>> FromManifest(
      const ManifestFile& manifest);

  /// \brief Create metadata instance for rewriting a manifest before commit.
  /// \param snapshot_id The snapshot ID for the copy operation.
  /// \return Inheritable metadata for copying.
  static Result<std::unique_ptr<InheritableMetadata>> ForCopy(int64_t snapshot_id);

 private:
  InheritableMetadataFactory() = default;
};

}  // namespace iceberg
