// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/apache/iceberg-go/puffin"
	"github.com/apache/iceberg-go/table/dv"
)

type fixtureBlob struct {
	referencedDataFile string
	positions          []uint64
	ranges             []positionRange
}

type positionRange struct {
	start uint64
	end   uint64
}

func writeFixture(outputDir, fileName, createdBy string, blobs []fixtureBlob) error {
	var output bytes.Buffer
	writer, err := puffin.NewWriter(&output)
	if err != nil {
		return err
	}
	if err := writer.SetCreatedBy(createdBy); err != nil {
		return err
	}

	for _, blob := range blobs {
		bitmap := dv.NewRoaringPositionBitmap()
		for _, position := range blob.positions {
			bitmap.Set(position)
		}
		for _, positionRange := range blob.ranges {
			bitmap.SetRange(positionRange.start, positionRange.end)
		}
		payload, err := dv.SerializeDV(bitmap)
		if err != nil {
			return err
		}
		_, err = writer.AddBlob(puffin.BlobMetadataInput{
			Type:           puffin.BlobTypeDeletionVector,
			SnapshotID:     -1,
			SequenceNumber: -1,
			Fields:         []int32{},
			Properties: map[string]string{
				"referenced-data-file": blob.referencedDataFile,
				"cardinality":          strconv.FormatInt(bitmap.Cardinality(), 10),
			},
		}, payload)
		if err != nil {
			return err
		}
	}

	if err := writer.Finish(); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outputDir, fileName), output.Bytes(), 0o644)
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run generate_go_fixtures.go OUTPUT_DIR")
		os.Exit(2)
	}
	outputDir := os.Args[1]
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		panic(err)
	}

	err := writeFixture(outputDir, "single-blob-dv.puffin",
		"iceberg-go test fixture", []fixtureBlob{{
			referencedDataFile: "data/test.parquet",
			positions:          []uint64{1, 3, 5, 7, 9},
		}})
	if err != nil {
		panic(err)
	}

	err = writeFixture(outputDir, "multi-blob-dv.puffin",
		"iceberg-go cross-language fixture", []fixtureBlob{
			{
				referencedDataFile: "s3://warehouse/db/table/data/go-file-001.parquet",
				positions: []uint64{
					0, 100, 200, (uint64(1) << 32) + 7,
				},
			},
			{
				referencedDataFile: "s3://warehouse/db/table/data/go-file-002.parquet",
				positions: []uint64{
					50, 150, (uint64(2) << 32) + 9,
				},
			},
		})
	if err != nil {
		panic(err)
	}

	position := func(bucket, container, value uint64) uint64 {
		return (bucket << 32) + (container << 16) + value
	}
	allContainerPositions := []uint64{
		position(0, 0, 5),
		position(0, 0, 7),
		position(1, 0, 10),
		position(1, 0, 20),
	}
	for bucket := uint64(0); bucket < 2; bucket++ {
		for value := uint64(0); value < 10000; value += 2 {
			allContainerPositions =
				append(allContainerPositions, position(bucket, 2, value))
		}
	}
	err = writeFixture(outputDir, "all-container-types-dv.puffin",
		"iceberg-go cross-language fixture", []fixtureBlob{{
			referencedDataFile: "s3://warehouse/db/table/data/all-containers.parquet",
			positions:          allContainerPositions,
			ranges: []positionRange{
				{start: position(0, 1, 1), end: position(0, 1, 1000)},
				{start: position(1, 1, 10), end: position(1, 1, 500)},
			},
		}})
	if err != nil {
		panic(err)
	}
}
