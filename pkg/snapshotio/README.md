# Snapshot copying

`github.com/milvus-io/milvus/pkg/v3/snapshotio` shares snapshot decoding,
physical file inventory, path rewriting and publication with Milvus Export.
Restore uses the same inventory and bounded copy executor while retaining its
own ID allocation, segment registration and visibility rules.

```go
copier, err := objectcopy.NewS3Copier(client, objectcopy.DefaultS3Options())
// Check err. client is an existing *minio.Client, owned by the caller.
engine, err := snapshotio.New(snapshotio.Dependencies{
    Source: sourceReader,
    Target: targetStore,
    Copier: copier,
    TargetStorageConfig: targetConfig,
}, snapshotio.Options{Concurrency: 128})
// Check err.
result, err := engine.CopySnapshot(ctx, snapshotio.CopyRequest{
    SourceMetadataURI: metadataURI,
    TargetRoot: exclusiveAttemptURI,
})
// Check err before exposing result.MetadataURI.
```

`Reader.Read` and `Store.Write` receive an `objectcopy.Object{Bucket, Key}`.
Keys are complete bucket-relative keys, without automatic root-prefix insertion.
Local adapters use absolute paths and an empty bucket. Stores return Milvus I/O
errors or the provider SDK's errors (classified by the adapter); a missing object
must be distinguishable from permission or network failure. A custom Copier
must report `BytesKnown: true` for successful data copies, without counting
unconfirmed writes. `NewS3Copier` handles this with the source HEAD request.
Clients, credentials, source pinning and cleanup remain caller-owned.

## Native manifests

Storage V3 inventory reads the pinned manifest bytes through Source, then calls
`DecodeManifest(data, basePath)`. The base is the original segment base key;
the decoder must return Go-owned inventory with complete resolved paths.
The decoder uses milvus-storage's backup/restore-only `loon_manifest_deserialize`
API, then expands relative paths using the native section layout. It preserves
external URIs and absolute local paths. Corrupt or unsupported manifests fail;
there is no empty-inventory or native-I/O fallback. Milvus server Export/Restore
retain their transaction reader; normal reads and commits never use raw serde.

`TargetStorageConfig` remains required for output URIs and publication recovery;
its bucket must match TargetRoot and the injected Target/Copier. The public
Engine no longer accepts source storage configuration or creates source clients.

Copies retain index definitions, build IDs and index files. Native manifests
are copied unchanged, and Avro paths are rewritten into the target layout.
The output uses the existing V5 snapshot format.

The native adapter is internal. Consumers need CGO and a compatible
`milvus-storage.pc`/library; production dependencies do not include Milvus
`internal/` packages or `libmilvus_core`. Pin the pkg revision and matching native
library together. Milvus integration fixtures remain under `internal/snapshotio/storage` and
additionally use Milvus core. Library unit tests have no server-package imports.
Run both from the repository root with `-tags dynamic,test`.

## Distributed use and recovery

1. `Open` loads top-level metadata once; `SegmentCount` defines `[0, count)`.
2. `CopyRange` processes a half-open segment range. Concurrent ranges on one
   Engine share its operation budget. Persist returned `RangeResult`s.
3. After all writers exit, `Prepare` validates complete nonoverlapping coverage
   and stages/readbacks metadata. Persist the returned `Publication`.
4. `Publish` needs only Target access and `TargetStorageConfig`. It can resume
   after source expiry and verifies final bytes before returning success.
   Conflicting bytes fail closed.

Use an exclusive target root per attempt and keep the source immutable throughout
copying. Do not rerun CopyRange/Prepare after publication; retry Publish instead.
The package does not provide a scheduler, source lease or distributed writer
fencing. Failed attempts may leave unpublished objects for caller cleanup.

`objectstorage/copy` can also be used independently of snapshots. Its S3 adapter
supports provider-side cross-bucket copy on one compatible service with a client
authorized for both buckets. It does not stream between unrelated services.
Multipart concurrency is per object; Engine concurrency bounds operations, not
the total HTTP request count. Set `DisableMultipart` for GCS's XML API.

## Shared segment execution

`EnumerateSegmentFiles` returns raw typed references and the pinned manifest
inventory. It preserves URI spelling and duplicates for caller selection and
validation. External Collection Restore excludes native URI references before
normalization; it still copies the manifest and snapshot-selected indexes.
No directory listing is used. Ordinary segments keep all enumerated references.

Export, Restore and Engine use `PlanFileCopies` for destination mapping and
collision checks, and `ExecuteFileCopies` for bounded copies and confirmed-write
accounting. Copy adapters report confirmation separately from errors so Restore
retains cleanup paths even when a successful provider write races a timeout.
Unknown sizes stay unknown; Export's sized adapter supplies verified sizes.
Export/Engine retain Avro rewriting and bundle publication. Restore retains ID
mapping, native index commits, metadata results and cleanup policy. Segment
scheduling remains outside the file executor; it does not merge their lifecycles.
