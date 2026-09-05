# Deletion vector interoperability fixtures

These fixtures verify deletion-vector compatibility against bytes produced by
implementations other than Iceberg C++.

## Java fixtures

The files under `java/` were copied byte-for-byte from Apache Iceberg Go commit
`3020adbbc3faff047da6f483f739f1b5e1de611b`, where they are consumed by
`table/dv/dv_cross_client_test.go`.

- `single-blob-dv.puffin` and `multi-blob-dv.puffin` were produced by Apache
  Iceberg Java using
  `iceberg-go/dev/dv-fixtures/GenerateDVFixtures.java`.

The committed Puffin bytes were reproduced against Apache Iceberg Java commit
`76d35b1e40f77edcad19646bb6afdd9f05249964`. To regenerate them, follow
`iceberg-go/dev/dv-fixtures/README.md` at the Iceberg Go commit above using
that Java revision, then copy the generated files into `java/`.

## Go fixtures

The files under `go/` are produced with the Iceberg Go Puffin and deletion
vector writers. From an Iceberg Go checkout at commit
`3020adbbc3faff047da6f483f739f1b5e1de611b`, run:

```bash
go run /path/to/iceberg-cpp/dev/dv-fixtures/generate_go_fixtures.go \
  /path/to/iceberg-cpp/src/iceberg/test/resources/deletion_vectors/go
```

The multi-blob fixture includes positions in three distinct high-32-bit
buckets. `all-container-types-dv.puffin` covers array, run, and bitmap
containers inside a complete Puffin file.

## Checksums

```text
ab8309671c0c5ef1956f4f1d7b907f4a69ffeb154ab77d5a4855d7dd4779108c  java/single-blob-dv.puffin
fed7edbb5a343a6c6fc4706ff4c213ab3f0a50916baeb228619f1f2c956f3f27  java/multi-blob-dv.puffin
dd293e827439cd22053a9356ae6d56f0d5d69e648b1850d39663cdc5c7ec5a77  go/single-blob-dv.puffin
000a77697d5ddce01e0242786b91fc15482a8b8f4fa3c8e214a936d7c2fc978b  go/multi-blob-dv.puffin
d9f374706891b4780f8a59f155a7cc4cae4161b6415578abb4790f6eed793a52  go/all-container-types-dv.puffin
```
