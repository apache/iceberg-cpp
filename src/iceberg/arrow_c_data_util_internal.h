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

#include <cerrno>
#include <concepts>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Cached state for ProjectBatch over one input/output schema pair.
class ICEBERG_EXPORT ProjectionContext {
 public:
  using ProjectBatchState = std::shared_ptr<void>;

  using ProjectBatchFunction = auto (*)(ArrowArray* input_batch,
                                        std::span<const int32_t> row_indices,
                                        ProjectionContext& projection)
      -> Result<ArrowArray>;

  /// \brief Register a custom implementation for ProjectBatch.
  static void RegisterProjectBatchFunction(ProjectBatchFunction project_batch_function);

  /// \brief Returns true when a custom implementation has been registered.
  static bool HasProjectBatchFunction();

  /// \brief Resolve the registered ProjectBatch implementation.
  static auto ResolveProjectBatchFunction() -> ProjectBatchFunction;

  /// \brief Build reusable projection state for a validated schema pair.
  ///
  /// \param input_schema Schema that describes every input batch.
  /// \param output_schema Final schema and column order requested by the caller.
  /// \param project_batch_function Optional implementation returned by
  ///   ProjectionContext::ResolveProjectBatchFunction, or nullptr to use the nanoarrow
  ///   path.
  /// \note It validates that output_schema selects or reorders complete top-level fields
  /// by field id. Nested pruning and type changes are rejected. The input_schema and
  /// output_schema passed to Make must outlive the context. ProjectBatch may lazily
  /// initialize backend cache; do not share one context across concurrent calls.
  static Result<ProjectionContext> Make(const Schema& input_schema,
                                        const Schema& output_schema,
                                        ProjectBatchFunction project_batch_function);

  ProjectionContext(ProjectionContext&&) noexcept;
  ProjectionContext& operator=(ProjectionContext&&) noexcept;
  ~ProjectionContext();

  ProjectionContext(const ProjectionContext&) = delete;
  ProjectionContext& operator=(const ProjectionContext&) = delete;

  const Schema& input_schema() const;

  const Schema& output_schema() const;

  const ArrowSchema& input_arrow_schema() const;

  const ArrowSchema& output_arrow_schema() const;

  std::span<const int> selected_field_indices() const;

  ProjectBatchFunction project_batch_function() const;

  ProjectBatchState& project_batch_state();

 private:
  ProjectionContext() = default;

  const Schema* input_schema_ = nullptr;
  const Schema* output_schema_ = nullptr;
  std::vector<int> selected_field_indices_;
  ArrowSchema input_arrow_schema_{};
  ArrowSchema output_arrow_schema_{};
  ProjectBatchFunction project_batch_function_ = nullptr;
  ProjectBatchState project_batch_state_;
};

/// \brief Concept for sources that can be wrapped as ArrowArrayStreams.
template <typename Source>
concept ArrowArrayStreamProvider = requires(Source& source) {
  { source.Close() } -> std::same_as<Status>;
  { source.Next() } -> std::same_as<Result<std::optional<ArrowArray>>>;
  { source.Schema() } -> std::same_as<Result<ArrowSchema>>;
};

namespace detail {

template <ArrowArrayStreamProvider Source>
struct ArrowArrayStreamPrivateData {
  std::unique_ptr<Source> source;
  std::string last_error;

  explicit ArrowArrayStreamPrivateData(std::unique_ptr<Source> src)
      : source(std::move(src)) {}

  ~ArrowArrayStreamPrivateData() {
    if (source != nullptr) {
      std::ignore = source->Close();
    }
  }
};

template <ArrowArrayStreamProvider Source>
int GetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
  if (stream == nullptr || stream->private_data == nullptr) {
    return EINVAL;
  }

  auto* private_data =
      static_cast<ArrowArrayStreamPrivateData<Source>*>(stream->private_data);
  auto schema_result = private_data->source->Schema();
  if (!schema_result.has_value()) {
    private_data->last_error = schema_result.error().message;
    std::memset(out, 0, sizeof(ArrowSchema));
    return EIO;
  }

  *out = std::move(schema_result.value());
  return 0;
}

template <ArrowArrayStreamProvider Source>
int GetNext(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  if (stream == nullptr || stream->private_data == nullptr) {
    return EINVAL;
  }

  auto* private_data =
      static_cast<ArrowArrayStreamPrivateData<Source>*>(stream->private_data);
  auto next_result = private_data->source->Next();
  if (!next_result.has_value()) {
    private_data->last_error = next_result.error().message;
    std::memset(out, 0, sizeof(ArrowArray));
    return EIO;
  }

  auto& optional_array = next_result.value();
  if (optional_array.has_value()) {
    *out = std::move(optional_array.value());
  } else {
    std::memset(out, 0, sizeof(ArrowArray));
  }

  return 0;
}

template <ArrowArrayStreamProvider Source>
const char* GetLastError(struct ArrowArrayStream* stream) {
  if (stream == nullptr || stream->private_data == nullptr) {
    return nullptr;
  }

  auto* private_data =
      static_cast<ArrowArrayStreamPrivateData<Source>*>(stream->private_data);
  return private_data->last_error.empty() ? nullptr : private_data->last_error.c_str();
}

template <ArrowArrayStreamProvider Source>
void Release(struct ArrowArrayStream* stream) {
  if (stream == nullptr || stream->private_data == nullptr) {
    return;
  }

  delete static_cast<ArrowArrayStreamPrivateData<Source>*>(stream->private_data);
  stream->private_data = nullptr;
  stream->release = nullptr;
}

}  // namespace detail

/// \brief Wrap an object with Close, Next, and Schema as an ArrowArrayStream.
template <ArrowArrayStreamProvider Source>
Result<ArrowArrayStream> MakeArrowArrayStream(std::unique_ptr<Source> source) {
  if (source == nullptr) {
    return InvalidArgument("Cannot make ArrowArrayStream from null source");
  }

  auto private_data =
      std::make_unique<detail::ArrowArrayStreamPrivateData<Source>>(std::move(source));
  ArrowArrayStream stream{.get_schema = detail::GetSchema<Source>,
                          .get_next = detail::GetNext<Source>,
                          .get_last_error = detail::GetLastError<Source>,
                          .release = detail::Release<Source>,
                          .private_data = private_data.release()};
  return stream;
}

/// \brief Wrap a Reader as an ArrowArrayStream.
ICEBERG_EXPORT Result<ArrowArrayStream> MakeArrowArrayStream(
    std::unique_ptr<Reader> reader);

/// \brief Project selected rows from a batch into complete top-level fields.
///
/// `input_batch` is consumed by this function. If the projection carries a registered
/// implementation, the call is delegated to it; otherwise the built-in nanoarrow
/// implementation is used. The projection must have been created for the stable schema
/// pair that describes the input batch and requested output. This function does not
/// revalidate schema compatibility on each batch.
///
/// \param input_batch Owned Arrow C Data batch to project.
/// \param row_indices Zero-based row positions to copy from `input_batch`.
/// \param projection Reusable schema/projection state created by
///   ProjectionContext::Make.
/// \return A newly owned ArrowArray matching `projection.output_schema()`.
ICEBERG_EXPORT Result<ArrowArray> ProjectBatch(ArrowArray* input_batch,
                                               std::span<const int32_t> row_indices,
                                               ProjectionContext& projection);

}  // namespace iceberg
