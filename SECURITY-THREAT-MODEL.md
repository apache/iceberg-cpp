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

# Apache Iceberg C++ Security Threat Model

This document describes the detailed security threat model for Apache
Iceberg C++. It is intended for maintainers and automated security triage.

## Purpose

Apache Iceberg C++ is primarily a native library implementing Iceberg table
format handling, catalog interactions, and related tooling for C++
applications and services. It is commonly embedded in larger systems that
provide their own authentication, authorization, and credential management.
Because of that deployment model, not every unsafe or surprising behavior is a
security vulnerability in Iceberg C++ itself.

This model is intended to answer:

- what Iceberg C++ generally treats as a security vulnerability
- what Iceberg C++ generally treats as correctness, hardening, or deployment
  work
- which boundaries are primarily owned by Iceberg C++ versus the surrounding
  catalog, application, or service
- which issue classes should be downgraded by default by scanners

## Scope

This model is scoped to the Apache Iceberg C++ repository itself:

- table format and metadata handling
- catalog and REST catalog clients
- transport, credential, and configuration handling implemented in this repo
- native parsing, memory management, and helper tooling shipped in this repo

It is not a general threat model for every application that embeds Iceberg
C++.

In particular, it does not attempt to define the complete security model for:

- applications or services that embed Iceberg C++
- storage-level authorization enforced outside Iceberg C++

## Security Goals

Iceberg C++ should:

- avoid exposing secrets or delegated credentials to principals that were not
  already trusted with them
- avoid creating new unauthorized capabilities in Iceberg C++-owned
  components
- avoid violating trust boundaries that Iceberg C++ itself owns, such as
  leaking auth, transport, or credential-bearing state across catalog or
  client boundaries in the same process
- avoid memory-safety violations triggered by untrusted input, including
  out-of-bounds access, use-after-free, and other memory corruption

Iceberg C++ does not aim to be the primary enforcement point for:

- user-to-user authorization inside the embedding application
- storage-level authorization
- service-side credential scoping performed by an external catalog

## Roles

### Operator

The operator configures the surrounding catalog, application, service, and
storage integration around Iceberg C++. This role is trusted to choose
endpoints, warehouses, storage integrations, and credentials.

### Catalog control plane

The catalog control plane resolves tables and supplies metadata, locations,
configuration, and delegated credentials to Iceberg C++. It may be
implemented by a REST catalog server or another catalog implementation.
Iceberg C++ assumes this control plane is trusted and outside its primary
security boundary.

### REST catalog client

The REST catalog client consumes catalog-provided metadata, configuration, and
credentials. Client-side bugs in routing, caching, or reuse may still be
security-relevant if they leak credential-bearing state across boundaries that
the Iceberg C++ client is expected to preserve.

### Embedding application

Applications and services embedding Iceberg C++ are responsible for their own
user-facing authorization boundaries unless Iceberg C++ explicitly documents
otherwise.

### Table writer or maintainer

This role may already have legitimate power to write or replace table
metadata, write or delete files, choose paths under an allowed warehouse or
table location, and invoke destructive maintenance operations. If a report
only shows a new way to achieve the same effect this role can already cause
legitimately, it is usually not a security issue in Iceberg C++.

## Trust Boundaries

### Boundary 1: operator-trusted configuration

The following are generally treated as trusted operator or deployment inputs:

- catalog properties
- endpoint configuration
- warehouse and storage roots
- transport wiring and credential configuration

If a report depends on the attacker controlling those values directly, it is
usually not a vulnerability in Iceberg C++ itself.

### Boundary 2: catalog-supplied metadata

Iceberg C++ often accepts metadata locations, table properties, namespace
properties, and related control-plane information from a catalog. By default,
Iceberg C++ treats those sources as trusted.

This means a malicious catalog supplying incorrect or malicious metadata is
usually not an Iceberg C++ vulnerability by itself.

### Boundary 3: REST catalog-supplied configuration and delegated storage access

In REST deployments, Iceberg C++ may also accept service endpoints,
configuration, and delegated storage access from the REST catalog server. By
default, those are treated as trusted control-plane inputs unless Iceberg C++
explicitly documents a stronger guarantee.

This means a malicious REST catalog server sending dangerous endpoints is
usually not an Iceberg C++ vulnerability by itself. It also means many
credential-selection bugs are often correctness or specification issues rather
than security boundary failures.

The major exception is secret exposure. If Iceberg C++ surfaces credentials or
secrets to a new audience that was not already trusted with them, that is
security-relevant.

### Boundary 4: storage-level authorization

Object store permissions are enforced by the storage provider and the
credentials the surrounding deployment chooses to hand to Iceberg C++.
Iceberg C++ is not the root authority for bucket- or object-level
authorization.

## In-Scope Security Vulnerabilities

The following categories are generally security-relevant in Iceberg C++ when
the report is credible and reproducible.

### 1. Secret or credential disclosure to a new audience

Examples include:

- catalog or storage credentials exposed through a user-visible surface
- one catalog's credentials or auth state leaking into another catalog or
  client

### 2. Iceberg C++-owned trust-boundary violations

Security issues exist when Iceberg C++ itself is expected to separate
catalogs, clients, or principals and fails to do so.

Examples include:

- process-global auth or transport state crossing catalog instances
- secret-bearing state from one principal reused for another principal within
  an Iceberg C++-owned boundary

### 3. Memory-safety violations from untrusted input

Out-of-bounds access, use-after-free, memory corruption, and similar native
memory-safety issues triggered by untrusted input are generally security-
relevant in Iceberg C++.

## Usually Out of Scope or Non-Security by Default

These categories may still be real bugs worth fixing, but they are not usually
security vulnerabilities in Iceberg C++ itself.

### 1. Correctness bugs

Examples include incorrect metadata handling, ambiguous matching semantics,
and logic bugs that do not create a new trust-boundary violation.

### 2. Parser hardening and malformed-input robustness without memory corruption

Malformed-input crashes, bounded allocation failures, and memory amplification
without memory corruption are usually treated as robustness or hardening work
rather than security issues in Iceberg C++ itself.

### 3. Malicious catalog or external service scenarios

Reports that require a malicious catalog or other external control-plane
service are usually outside Iceberg C++'s primary security boundary.

### 4. Equivalent-harm reports

If the actor already has a legitimate capability that can cause the same harm,
the new path is usually not a security issue.

## Scanner Calibration Rules

A scanner targeting Iceberg C++ should treat a finding as higher-confidence
only if it plausibly shows one of the following:

- exposure of a secret or delegated credential to a new audience
- creation of a new unauthorized capability in an Iceberg C++-owned component
- violation of an Iceberg C++-owned trust boundary rather than a surrounding
  catalog, application, service, or operator boundary
- memory corruption or other native memory-safety violations triggered by
  untrusted input

A finding should be downgraded or rejected by default if it instead depends
primarily on:

- malformed-input robustness or denial-of-service behavior without memory
  corruption
- a malicious catalog or external service
- a principal that already has equivalent power through legitimate write or
  maintenance capabilities
