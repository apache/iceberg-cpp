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

#include "iceberg/compaction_planner.h"

#include <algorithm>
#include <cmath>
#include <compare>
#include <iterator>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <utility>

#include "iceberg/expression/literal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/table_scan.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/int128.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

struct CanonicalPartitionKey {
  int32_t spec_id;
  std::string values;

  auto operator<=>(const CanonicalPartitionKey&) const = default;
};

struct Candidate {
  CompactionFile file;
  bool has_delete_pressure;
};

struct PartitionCandidates {
  int32_t spec_id;
  std::vector<Candidate> files;
};

enum class Comparison {
  kLess,
  kEqual,
  kGreater,
};

Status ValidateConfig(const CompactionPlannerConfig& config) {
  ICEBERG_PRECHECK(config.target_file_size_bytes > 0,
                   "target_file_size_bytes must be greater than zero");
  ICEBERG_PRECHECK(std::isfinite(config.min_file_size_ratio) &&
                       config.min_file_size_ratio >= 0 && config.min_file_size_ratio <= 1,
                   "min_file_size_ratio must be finite and in [0, 1]");
  ICEBERG_PRECHECK(
      std::isfinite(config.max_file_size_ratio) && config.max_file_size_ratio >= 1,
      "max_file_size_ratio must be finite and at least 1");
  ICEBERG_PRECHECK(config.min_input_files > 0,
                   "min_input_files must be greater than zero");
  ICEBERG_PRECHECK(config.delete_file_threshold >= 0,
                   "delete_file_threshold must not be negative");
  ICEBERG_PRECHECK(std::isfinite(config.delete_ratio_threshold) &&
                       config.delete_ratio_threshold >= 0 &&
                       config.delete_ratio_threshold <= 1,
                   "delete_ratio_threshold must be finite and in [0, 1]");
  return {};
}

Result<int64_t> CheckedAddNonNegative(int64_t lhs, int64_t rhs,
                                      std::string_view description) {
  ICEBERG_PRECHECK(lhs >= 0 && rhs >= 0, "{} must not be negative", description);
  ICEBERG_PRECHECK(lhs <= std::numeric_limits<int64_t>::max() - rhs, "{} overflow",
                   description);
  return lhs + rhs;
}

Comparison Compare(uint128_t lhs, uint128_t rhs) {
  if (lhs < rhs) {
    return Comparison::kLess;
  }
  if (lhs > rhs) {
    return Comparison::kGreater;
  }
  return Comparison::kEqual;
}

Comparison CompareIntegerToScaled(int64_t value, int64_t scale, double factor) {
  if (factor == 0) {
    return Compare(static_cast<uint128_t>(value), uint128_t{0});
  }

  constexpr int64_t kLargestExactDoubleInteger = int64_t{1} << 53;
  if (value <= kLargestExactDoubleInteger && scale <= kLargestExactDoubleInteger) {
    // Preserve normal double threshold semantics while both integers are exact.
    const double scaled = static_cast<double>(scale) * factor;
    if (static_cast<double>(value) < scaled) {
      return Comparison::kLess;
    }
    if (static_cast<double>(value) > scaled) {
      return Comparison::kGreater;
    }
    return Comparison::kEqual;
  }

  // Avoid lossy integer-to-double conversion by comparing against the exact binary
  // rational represented by factor.
  int exponent = 0;
  const double fraction = std::frexp(factor, &exponent);
  constexpr int kDoubleDigits = std::numeric_limits<double>::digits;
  const auto significand = static_cast<uint64_t>(std::ldexp(fraction, kDoubleDigits));
  const int shift = exponent - kDoubleDigits;
  const uint128_t product =
      static_cast<uint128_t>(scale) * static_cast<uint128_t>(significand);
  constexpr uint128_t kMax = ~uint128_t{0};
  if (product == 0) {
    return Compare(static_cast<uint128_t>(value), uint128_t{0});
  }

  if (shift >= 0) {
    if (shift >= 128 || product > (kMax >> shift)) {
      return Comparison::kLess;
    }
    return Compare(static_cast<uint128_t>(value), product << shift);
  }

  const int left_shift = -shift;
  const uint128_t lhs = static_cast<uint128_t>(value);
  if (lhs == 0) {
    return Comparison::kLess;
  }
  if (left_shift >= 128 || lhs > (kMax >> left_shift)) {
    return Comparison::kGreater;
  }
  return Compare(lhs << left_shift, product);
}

void AppendFramed(std::string& output, std::string_view value) {
  output.append(std::to_string(value.size()));
  output.push_back(':');
  output.append(value);
}

Result<CanonicalPartitionKey> MakePartitionKey(const DataFile& data_file) {
  ICEBERG_PRECHECK(data_file.partition_spec_id.has_value(),
                   "Data file '{}' is missing partition_spec_id", data_file.file_path);

  std::string encoded;
  encoded.append(std::to_string(data_file.partition.num_fields()));
  encoded.push_back(':');
  for (const auto& literal : data_file.partition.values()) {
    AppendFramed(encoded, literal.type()->ToString());
    if (literal.IsNull()) {
      encoded.push_back('N');
      continue;
    }

    encoded.push_back('V');
    if (literal.IsNaN()) {
      encoded.push_back('N');
      continue;
    }

    encoded.push_back('B');
    ICEBERG_ASSIGN_OR_RAISE(auto bytes, literal.Serialize());
    std::string_view serialized;
    if (!bytes.empty()) {
      serialized =
          std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    AppendFramed(encoded, serialized);
  }
  return CanonicalPartitionKey{.spec_id = *data_file.partition_spec_id,
                               .values = std::move(encoded)};
}

Result<CompactionFile> BuildCompactionFile(
    const std::shared_ptr<FileScanTask>& scan_task) {
  CompactionFile result{.scan_task = scan_task};
  const auto& data_file = scan_task->data_file();
  ICEBERG_PRECHECK(data_file->record_count >= 0,
                   "Data file '{}' has negative record count", data_file->file_path);
  ICEBERG_PRECHECK(data_file->file_size_in_bytes >= 0,
                   "Data file '{}' has negative file size", data_file->file_path);

  DeleteFileSet applicable_deletes;
  for (const auto& delete_file : scan_task->delete_files()) {
    ICEBERG_PRECHECK(delete_file != nullptr, "Data file '{}' has a null delete file",
                     data_file->file_path);
    if (delete_file->content != DataFile::Content::kPositionDeletes) {
      continue;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto referenced_file,
                            ContentFileUtil::ReferencedDataFile(*delete_file));
    if (!referenced_file.has_value() || *referenced_file != data_file->file_path) {
      continue;
    }

    if (!applicable_deletes.insert(delete_file).second) {
      continue;
    }

    ICEBERG_PRECHECK(delete_file->record_count >= 0,
                     "Delete file '{}' has negative record count",
                     delete_file->file_path);
    if (delete_file->IsDeletionVector()) {
      ICEBERG_PRECHECK(
          delete_file->record_count <= data_file->record_count,
          "Deletion vector '{}' cardinality {} exceeds data file '{}' record count {}",
          delete_file->file_path, delete_file->record_count, data_file->file_path,
          data_file->record_count);
    }

    ICEBERG_ASSIGN_OR_RAISE(result.file_scoped_delete_count,
                            CheckedAddNonNegative(result.file_scoped_delete_count, 1,
                                                  "File-scoped delete-file count"));
    ICEBERG_ASSIGN_OR_RAISE(result.file_scoped_delete_record_count,
                            CheckedAddNonNegative(result.file_scoped_delete_record_count,
                                                  delete_file->record_count,
                                                  "File-scoped delete-record count"));
  }

  result.file_scoped_delete_record_count =
      std::min(result.file_scoped_delete_record_count, data_file->record_count);
  return result;
}

bool CompactionFileLess(const CompactionFile& lhs, const CompactionFile& rhs) {
  return lhs.scan_task->data_file()->file_path < rhs.scan_task->data_file()->file_path;
}

bool CandidateLess(const Candidate& lhs, const Candidate& rhs) {
  return CompactionFileLess(lhs.file, rhs.file);
}

bool PackingCandidateLess(const Candidate& lhs, const Candidate& rhs) {
  const int64_t lhs_size = lhs.file.scan_task->data_file()->file_size_in_bytes;
  const int64_t rhs_size = rhs.file.scan_task->data_file()->file_size_in_bytes;
  return lhs_size != rhs_size ? lhs_size > rhs_size : CandidateLess(lhs, rhs);
}

Result<void> AddToGroup(CompactionGroup& group, Candidate candidate,
                        bool& has_delete_pressure) {
  const auto& data_file = *candidate.file.scan_task->data_file();
  ICEBERG_ASSIGN_OR_RAISE(
      group.data_file_size_bytes,
      CheckedAddNonNegative(group.data_file_size_bytes, data_file.file_size_in_bytes,
                            "Compaction group data-file size"));
  ICEBERG_ASSIGN_OR_RAISE(
      group.file_scoped_delete_record_count,
      CheckedAddNonNegative(group.file_scoped_delete_record_count,
                            candidate.file.file_scoped_delete_record_count,
                            "Compaction group delete-record count"));
  has_delete_pressure |= candidate.has_delete_pressure;
  group.files.push_back(std::move(candidate.file));
  return {};
}

struct PendingGroup {
  CompactionGroup group;
  bool has_delete_pressure = false;
};

Result<std::vector<CompactionGroup>> PackPartition(
    PartitionCandidates partition, const CompactionPlannerConfig& config) {
  const auto representative = std::ranges::min_element(partition.files, CandidateLess)
                                  ->file.scan_task->data_file()
                                  ->partition;
  std::ranges::sort(partition.files, PackingCandidateLess);

  std::vector<PendingGroup> bins;
  for (auto& candidate : partition.files) {
    const int64_t file_size = candidate.file.scan_task->data_file()->file_size_in_bytes;
    size_t best_bin = bins.size();
    int64_t best_remaining = std::numeric_limits<int64_t>::max();
    for (size_t i = 0; i < bins.size(); ++i) {
      const int64_t bin_size = bins[i].group.data_file_size_bytes;
      if (bin_size > config.target_file_size_bytes ||
          file_size > config.target_file_size_bytes - bin_size) {
        continue;
      }

      const int64_t remaining = config.target_file_size_bytes - bin_size - file_size;
      if (remaining < best_remaining) {
        best_bin = i;
        best_remaining = remaining;
      }
    }

    if (best_bin == bins.size()) {
      bins.push_back(PendingGroup{
          .group = CompactionGroup{.partition_spec_id = partition.spec_id,
                                   .partition = representative},
      });
      best_bin = bins.size() - 1;
    }
    ICEBERG_RETURN_UNEXPECTED(AddToGroup(bins[best_bin].group, std::move(candidate),
                                         bins[best_bin].has_delete_pressure));
  }

  std::vector<CompactionGroup> groups;
  for (auto& bin : bins) {
    if (!bin.has_delete_pressure && bin.group.files.size() < config.min_input_files) {
      continue;
    }
    std::ranges::sort(bin.group.files, CompactionFileLess);
    groups.push_back(std::move(bin.group));
  }
  std::ranges::sort(groups, [](const CompactionGroup& lhs, const CompactionGroup& rhs) {
    return CompactionFileLess(lhs.files.front(), rhs.files.front());
  });
  return groups;
}

}  // namespace

Result<CompactionPlan> CompactionPlanner::Plan(
    int64_t source_snapshot_id, std::span<const std::shared_ptr<FileScanTask>> scan_tasks,
    const CompactionPlannerConfig& config) {
  ICEBERG_PRECHECK(source_snapshot_id >= 0, "source_snapshot_id must not be negative");
  ICEBERG_RETURN_UNEXPECTED(ValidateConfig(config));
  std::map<CanonicalPartitionKey, PartitionCandidates> partitions;
  DataFileSet seen_data_files;

  for (const auto& scan_task : scan_tasks) {
    ICEBERG_PRECHECK(scan_task != nullptr, "File scan task must not be null");
    ICEBERG_PRECHECK(scan_task->data_file() != nullptr,
                     "File scan task data file must not be null");
    const auto& data_file = *scan_task->data_file();
    ICEBERG_PRECHECK(seen_data_files.insert(scan_task->data_file()).second,
                     "Duplicate scan task for data file '{}'", data_file.file_path);
    ICEBERG_ASSIGN_OR_RAISE(auto partition_key, MakePartitionKey(data_file));
    ICEBERG_ASSIGN_OR_RAISE(auto file, BuildCompactionFile(scan_task));

    const bool is_small = CompareIntegerToScaled(
                              data_file.file_size_in_bytes, config.target_file_size_bytes,
                              config.min_file_size_ratio) == Comparison::kLess;
    const bool is_oversized =
        CompareIntegerToScaled(data_file.file_size_in_bytes,
                               config.target_file_size_bytes,
                               config.max_file_size_ratio) == Comparison::kGreater;
    const bool has_many_delete_files =
        config.delete_file_threshold > 0 &&
        file.file_scoped_delete_count >= config.delete_file_threshold;
    const bool has_high_delete_ratio =
        config.delete_ratio_threshold > 0 && data_file.record_count > 0 &&
        CompareIntegerToScaled(file.file_scoped_delete_record_count,
                               data_file.record_count,
                               config.delete_ratio_threshold) != Comparison::kLess;
    const bool has_delete_pressure = has_many_delete_files || has_high_delete_ratio;
    const bool should_compact =
        is_oversized ? has_delete_pressure : is_small || has_delete_pressure;
    if (!should_compact) {
      continue;
    }

    auto partition =
        partitions
            .try_emplace(std::move(partition_key),
                         PartitionCandidates{.spec_id = *data_file.partition_spec_id})
            .first;
    partition->second.files.push_back(
        Candidate{.file = std::move(file), .has_delete_pressure = has_delete_pressure});
  }

  CompactionPlan plan{.source_snapshot_id = source_snapshot_id};
  for (auto& [_, partition] : partitions) {
    ICEBERG_ASSIGN_OR_RAISE(auto groups, PackPartition(std::move(partition), config));
    plan.groups.insert(plan.groups.end(), std::make_move_iterator(groups.begin()),
                       std::make_move_iterator(groups.end()));
  }
  return plan;
}

}  // namespace iceberg
