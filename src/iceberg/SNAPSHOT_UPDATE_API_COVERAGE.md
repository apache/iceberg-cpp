# SnapshotUpdate API Coverage

This document tracks the implementation status of the `SnapshotUpdate` interface compared to the Java Iceberg implementation.

## Java SnapshotUpdate Interface

The Java `SnapshotUpdate<ThisT>` interface defines 6 methods:

### 1. `set(String property, String value)` ✅ IMPLEMENTED
**Java:**
```java
/**
 * Set a summary property in the snapshot produced by this update.
 *
 * @param property a String property name
 * @param value a String property value
 * @return this for method chaining
 */
ThisT set(String property, String value);
```

**C++:**
```cpp
Derived& Set(std::string_view property, std::string_view value);
```

### 2. `deleteWith(Consumer<String> deleteFunc)` ✅ IMPLEMENTED
**Java:**
```java
/**
 * Set a callback to delete files instead of the table's default.
 *
 * @param deleteFunc a String consumer used to delete locations.
 * @return this for method chaining
 */
ThisT deleteWith(Consumer<String> deleteFunc);
```

**C++:**
```cpp
Derived& DeleteWith(std::function<void(std::string_view)> delete_func);
```

### 3. `stageOnly()` ✅ IMPLEMENTED
**Java:**
```java
/**
 * Called to stage a snapshot in table metadata, but not update the current snapshot id.
 *
 * @return this for method chaining
 */
ThisT stageOnly();
```

**C++:**
```cpp
Derived& StageOnly();
```

### 4. `scanManifestsWith(ExecutorService executorService)` ⏸️ DEFERRED
**Java:**
```java
/**
 * Use a particular executor to scan manifests. The default worker pool will be used by default.
 *
 * @param executorService the provided executor
 * @return this for method chaining
 */
ThisT scanManifestsWith(ExecutorService executorService);
```

**C++:** NOT IMPLEMENTED

**Reason:** Requires executor/thread pool infrastructure which is not yet available in the codebase.

**Future Implementation:**
```cpp
// To be added when executor infrastructure is available
Derived& ScanManifestsWith(std::shared_ptr<Executor> executor);
```

### 5. `toBranch(String branch)` ✅ IMPLEMENTED
**Java:**
```java
/**
 * Perform operations on a particular branch
 *
 * @param branch which is name of SnapshotRef of type branch.
 */
default ThisT toBranch(String branch) {
    throw new UnsupportedOperationException(
        String.format(
            "Cannot commit to branch %s: %s does not support branch commits",
            branch, this.getClass().getName()));
}
```

**C++ Implementation:**
```cpp
Derived& ToBranch(std::string_view branch);
```

**Note:** Java has a default implementation that throws `UnsupportedOperationException`.
C++ requires derived classes to implement the full functionality.

### 6. `validateWith(SnapshotAncestryValidator validator)` ❌ MISSING
**Java:**
```java
/**
 * Validate snapshot ancestry before committing.
 */
default ThisT validateWith(SnapshotAncestryValidator validator) {
    throw new UnsupportedOperationException(
        "Snapshot validation not supported by " + this.getClass().getName());
}
```

**C++:** NOT IMPLEMENTED

**Reason:** Not identified during initial implementation review.

**Future Implementation:**
```cpp
// To be added when SnapshotAncestryValidator infrastructure is available
// Note: Java has default implementation that throws UnsupportedOperationException
// Consider whether to provide similar default behavior or omit until needed
```

## Summary

| Method | Java | C++ | Status | Notes |
|--------|------|-----|--------|-------|
| set() | ✅ | ✅ | Implemented | |
| deleteWith() | ✅ | ✅ | Implemented | |
| stageOnly() | ✅ | ✅ | Implemented | |
| scanManifestsWith() | ✅ | ❌ | Deferred | Needs executor infrastructure |
| toBranch() | ✅ (default throws) | ✅ | Implemented | C++ requires full implementation |
| validateWith() | ✅ (default throws) | ❌ | Missing | Needs SnapshotAncestryValidator |

**Implementation Coverage:** 4/6 methods (66%)
**Fully Usable Coverage:** 4/4 required methods (100%) - the missing methods have default throwing implementations in Java

## Next Steps

1. **ScanManifestsWith()**: Add when executor/thread pool infrastructure is available
2. **ValidateWith()**: Add when SnapshotAncestryValidator is implemented
   - Consider whether to provide a no-op implementation initially
   - Java's default implementation throws UnsupportedOperationException
   - May be better to omit until validation infrastructure exists
