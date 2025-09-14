# abortable-bytes

A tiny Scala/Kyo POC that makes **cloud uploads cancel-safe**. Stream large files to Google Cloud Storage (via emulator) and prove:

- **Cancellation** does **not finalize** the object (session left open).
- **Blocking I/O** uses `IO.interruptibleMany` (preemptible writes/closes).
- **Resumability** works deterministically with a small, simple **replay buffer** + `WriteChannel.capture/restore`.

> Status: **GCS (IO-specific sink) working with fake-gcs-server**. This repo focuses on GCS to keep the POC minimal and crystal clear.

---

## What's here

- `GcsUploadSink` — IO-specific resumable upload using:
  - `com.google.cloud.storage.WriteChannel.capture/restore()`
  - chunked streaming via FS2
  - cancel preemption via `IO.interruptibleMany`
  - deterministic failure injection (see `FAIL_AFTER_CHUNKS`)
  - **replay buffer** for bytes written since last checkpoint to keep **writer offset** and **source position** consistent after restore
- `KyoInterOp` — tiny bridge to run Kyo `< Async & Abort[Throwable] & Env[Ctx] >` programs as `IO`.
- `Main` — CLI to demo success, cancel, and resume scenarios.

---

## Why it’s interesting

- **Cancel-safe**: stopping the process/fiber before `close()` leaves the resumable session unfinalized → **no object** appears.
- **Deterministic resume**: inject a retryable error at a known **chunk index** and watch the sink restore, **replay buffered bytes** since the last `capture()`, and continue.
- **Minimality**: no re-implementation of GCS; we use the official Java client correctly at the effect boundaries.

---

## Requirements

- Scala 3.7.0
- Java 17+
- sbt 1.9+
- Docker (for the GCS emulator)

---

## Start the emulator

```
docker rm -f fake-gcs-server 2>/dev/null || true
# Expose both HTTPS(4443) and HTTP(8000); we use HTTP to avoid TLS fuss
docker run -d --name fake-gcs-server \
  -p 4443:4443 -p 8000:8000 \
  fsouza/fake-gcs-server -scheme both

export GCS_HOST="http://localhost:8000"
export GCP_PROJECT="demo"

# Create bucket
curl -s -X POST "$GCS_HOST/storage/v1/b?project=$GCP_PROJECT" \
  -H "Content-Type: application/json" \
  -d '{"name":"sample-gcs-bucket"}' | jq .
```

---

## Build

```
sbt clean compile
```

---

## Run the demos

### 1) Baseline (successful upload)

```
GCS_HOST=$GCS_HOST GCP_PROJECT=$GCP_PROJECT \
  sbt "runMain vim.poc.Main gcs gs://sample-gcs-bucket/big.pdf /path/to/221mb.pdf"
```

**Observe**: console logs `GCS upload start` → `GCS upload done ← bytes=…`. The object should exist:

```
curl -s "$GCS_HOST/storage/v1/b/sample-gcs-bucket/o/big.pdf" | jq '{name,size}'
```

### 2) Cancellation (no finalize)

Throttle a bit so cancel lands mid-stream:

```
export GCS_CHUNK_MB=8
export GCS_THROTTLE_MS_PER_CHUNK=150
GCS_HOST=$GCS_HOST GCP_PROJECT=$GCP_PROJECT \
  sbt "runMain vim.poc.Main gcs gs://sample-gcs-bucket/cancel.pdf /path/to/221mb.pdf --cancel-after-ms 1000"
```

**Observe**: run cancels; **no object** `cancel.pdf` in listing.

### 3) Deterministic resume (retry + capture/restore + replay)

```
export CAPTURE_EVERY_CHUNKS=4        # checkpoint more often for the demo
export FAIL_AFTER_CHUNKS=10          # inject a retryable error at chunk index 10
export GCS_CHUNK_MB=8
GCS_HOST=$GCS_HOST GCP_PROJECT=$GCP_PROJECT \
  sbt "runMain vim.poc.Main gcs gs://sample-gcs-bucket/resume.pdf /path/to/221mb.pdf"
```

**Observe**:
- Logs show pre-write captures at chunks 4, 8, 12 ..
- At chunk 10: `retry after error.. replaying N buffered chunks` (usually 2)
- Final `GCS upload done ← bytes=…` and object `resume.pdf` size matches source.

> If you still hit a finalize size mismatch with the emulator, increase capture frequency (e.g., `CAPTURE_EVERY_CHUNKS=2`) so the replay window is tiny. The algorithm already replays bytes since last capture; very infrequent checkpoints can still trip emulator edge-cases right at stream end.

---

## Environment variables

- `GCS_HOST` - emulator URL (e.g., `http://localhost:8000`)
- `GCP_PROJECT` - project id for emulator
- `GCS_CHUNK_MB` - chunk size in MiB (default 8)
- `GCS_THROTTLE_MS_PER_CHUNK` - sleep per chunk (only in `Main`, helps demo cancel/races)
- `CAPTURE_EVERY_CHUNKS` - capture frequency (default 16)
- `FAIL_AFTER_CHUNKS` - inject retryable failure at this 0-based chunk index (demo only)

---

## Design (current POC)

- **IO-specific sink**: `GcsUploadSink` implements `CloudUploadSink[IO]` to keep boundaries simple and remove generic `F` shuttling/dispatchers.
- **Small modules inside the sink**:
  - `newWriter / closeWriter / writeChunk` - effect-wrapped SDK calls
  - `captureWriter / restoreWriter` - resumable session control
  - `writeWithRetry(cur, bytes, idx, attempt)` — single responsibility: write *this* chunk, catch retryable errors, **restore**, **replay buffered chunks**, and retry
  - streaming fold only maintains `(writer, totalBytes, chunkIndex)` and performs **pre-write** capture at boundaries; pushes successful chunks into the **replay buffer**; clears buffer on capture
- **Kyo**: the core program is `< Async & Abort[Throwable] & Env[UploadCtx] >` and is run into `IO` via a narrow interop object.

---

## What we tried & lessons

- Capturing **after** writing a boundary chunk caused end-of-stream size mismatches when a failure was injected near the end; fixed by switching to **pre-write** capture.
- Restoring writer state without replaying the data since last capture is incorrect; we added a small **replay buffer** and re-send those chunks after restore.
- Using `IO.blocking` alone makes cancel preemption brittle; `IO.interruptibleMany` significantly improves cancel behaviour for `write` and `close`.

---

## Scope for improvement

- **Cleaner composition**: extract a tiny `Resumable` algebra with methods `capture()`, `restore()`, `replay(bufs)`, `write(chunk)`, `finalize()`, then implement GCS in terms of it. The current sink is intentionally compact but can be refactored for readability.
- **Backoff knobs**: `MAX_RETRIES`, `RETRY_BACKOFF_MS` with jitter.
- **Session-aware resume**: optionally query upload status to compute server-side offset when available, then realign the replay buffer dynamically.
- **Integrity**: verify `md5Hash` / `crc32c` returned by server vs. local.
- **Real cloud runs**: add runner scripts against real GCS/S3 (not just emulators) behind explicit env flags.

---

## Caveats

- Emulators don’t perfectly reproduce every wire behaviour around resumables. The POC keeps the algorithm conservative (pre-write capture + replay-since-capture). If you see a size mismatch at finalize, **increase `CAPTURE_EVERY_CHUNKS`** to shrink the replay window.

---

## License
MIT
This POC builds on the Google Cloud Java client, Cats Effect, FS2, Kyo, and fake-gcs-server. Refer to each project for respective licenses.