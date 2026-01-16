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

#include "iceberg/data/data_writer.h"

namespace iceberg {

class DataWriter::Impl {
 public:
};

DataWriter::~DataWriter() = default;

Status DataWriter::Write(ArrowArray* data) { return NotImplemented(""); }

Result<int64_t> DataWriter::Length() const { return NotImplemented(""); }

Status DataWriter::Close() { return NotImplemented(""); }

Result<FileWriter::WriteResult> DataWriter::Metadata() { return NotImplemented(""); }

}  // namespace iceberg
