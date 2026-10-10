<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Parquet split fixture with omitted row-group offsets

`split-without-row-group-offsets.parquet` contains a required `int32` column
named `id`, with Iceberg field ID 1 and values 1 through 6. Its three row groups
contain two rows each, with dictionary pages before their first data pages.
The optional `RowGroup.file_offset` field is absent from every row group.

The fixture was generated with PyArrow 12.0.1 using the following write options:

```python
schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False, metadata={b"PARQUET:field_id": b"1"})
])
table = pa.Table.from_arrays([pa.array([1, 2, 3, 4, 5, 6], type=pa.int32())], schema=schema)
pq.write_table(table, path, row_group_size=2, compression="NONE",
               use_dictionary=True, version="2.6", store_schema=False)
```

The footer was then decoded with Thrift's compact protocol, field 5
(`file_offset`) was removed from each entry of `FileMetaData.row_groups`, and
the footer was serialized again. The original data pages were retained and
the four-byte footer length was updated. A PyArrow read of the resulting file
was checked against the original table.

This fixture tests that split selection uses the first column's page offsets
even when the optional row-group offset is unavailable.
