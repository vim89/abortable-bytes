package vim.poc.gcs

import cats.arrow.FunctionK
import cats.effect.IO as CatsIO
import cats.effect.kernel.{Outcome, Async as CEAsync}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import fs2.Stream
import kyo.{<, Env}
import com.google.auth.Credentials
import com.google.cloud.{NoCredentials, WriteChannel}
import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageException, StorageOptions}
import vim.poc.{CloudUploadSink, CloudUri, KyoInterOp, UploadResult}

import java.io.*
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference

/** Resumable GCS upload sink using `WriteChannel.capture/restore`. Generic in `F` for the public API, translating
  * `Stream[F, *]` to `Stream[IO, *]` at the boundary with a `Dispatcher`, then converting the final `IO` back to `F`.
  *   - uses `KyoInterOp.FX` (the canonical effect set) for the Kyo program, and
  *   - obtains request-scoped `UploadCtx` via `Env.get[UploadCtx]`, passing it in with `KyoInterOp.run(ctx)(program)`.
  */
final class GcsUploadSink[F[_]](storage: Storage, chunkSize: Int = 8 * 1024 * 1024)(using CEAsync[F])
    extends CloudUploadSink[F] {

  // capture/restore kept in-memory for demo (persist externally to survive process restarts)
  private final case class Capture(bytes: Array[Byte])

  private val captureRef = new AtomicReference[Option[Capture]](None)

  // Low-level blocking helpers in IO
  private def newWriter(blobInfo: BlobInfo): CatsIO[WriteChannel] =
    CatsIO.blocking {
      val w = storage.writer(blobInfo) // resumable for large payloads
      w.setChunkSize(chunkSize)
      w
    }

  private def restoreWriter(cap: Capture): CatsIO[WriteChannel] =
    CatsIO.blocking {
      val bais  = new ByteArrayInputStream(cap.bytes)
      val ois   = new ObjectInputStream(bais)
      val state = ois.readObject().asInstanceOf[com.google.cloud.RestorableState[WriteChannel]]
      state.restore()
    }

  private def captureWriter(w: WriteChannel): CatsIO[Unit] =
    CatsIO.blocking {
      val state = w.capture() // capture/restore support in GCS client
      val baos  = new ByteArrayOutputStream()
      val oos   = new ObjectOutputStream(baos)
      oos.writeObject(state)
      oos.flush()
      captureRef.set(Some(Capture(baos.toByteArray)))
    }

  private def writeChunk(w: WriteChannel, arr: Array[Byte]): CatsIO[Int] =
    // interruptible so cancellation can preempt mid-call if the channel honors interruption
    CatsIO.interruptibleMany(w.write(ByteBuffer.wrap(arr)))

  private def closeWriter(w: WriteChannel): CatsIO[Unit] =
    CatsIO.interruptibleMany(w.close())

  private def isRetryable(t: Throwable): Boolean = t match
    case e: StorageException =>
      val c = e.getCode
      c == 408 || c == 429 || (c >= 500 && c < 600)
    case _ => false

  /** Public API Upload a byte stream to gs://bucket/key; returns exact bytes (generation/etag not exposed on close). */
  def upload(bytes: Stream[F, Byte], dest: CloudUri): F[UploadResult] =
    val blobId   = BlobId.of(dest.bucket, dest.key)
    val blobInfo = BlobInfo.newBuilder(blobId).build()

    // Use a Dispatcher to (1) translate Stream[F, *] -> Stream[IO, *] and (2) convert IO back to F
    Dispatcher.parallel[F].use { dispatcher =>
      // F ~> IO (safe) using Dispatcher’s unsafeToFuture wrapped in IO.fromFuture
      val toIO: FunctionK[F, CatsIO] = new FunctionK[F, CatsIO]:
        def apply[A](fa: F[A]): CatsIO[A] = CatsIO.fromFuture(CatsIO(dispatcher.unsafeToFuture(fa)))

      val bytesIO: Stream[CatsIO, Byte] = bytes.translate(toIO)

      // Provide request-scoped context to the Kyo program
      val ctx = KyoInterOp.UploadCtx(
        traceId = java.util.UUID.randomUUID().toString,
        auditId = s"${dest.bucket}/${dest.key}"
      )

      // Kyo program with the canonical effect set `FX` and `UploadCtx` in Env
      val program: UploadResult < KyoInterOp.FX =
        for
          ctxEff <- Env.get[KyoInterOp.UploadCtx]
          _ <- KyoInterOp
            .get(CatsIO.println(s"[trace=${ctxEff.traceId}] GCS upload start -> ${dest.bucket}/${dest.key}"))

          // acquire writer
          w0 <- KyoInterOp.get(newWriter(blobInfo))

          // process the stream in IO, lifted into the Kyo program
          lastState <- KyoInterOp.get {
            bytesIO
              .chunkN(chunkSize, allowFewer = true)
              .evalMapAccumulate((w0, 0L)) { case ((w, total), chunk) =>
                val arr = chunk.toArray
                writeChunk(w, arr).attempt.flatMap {
                  case Right(n) =>
                    val nextTotal = total + n.toLong
                    val doCapture = (nextTotal / chunkSize) % 16 == 0
                    val cap       = if doCapture then captureWriter(w) else CatsIO.unit
                    cap.as(((w, nextTotal), (w, nextTotal)))
                  case Left(e) if isRetryable(e) =>
                    val nextW = captureRef.get() match
                      case Some(cap) => restoreWriter(cap)
                      case None      => newWriter(blobInfo)
                    nextW.map(nw => ((nw, total), (nw, total)))
                  case Left(e) => CatsIO.raiseError(e)
                }
              }
              .map(_._2) // stream of (writer,total)
              .compile
              .lastOrError
          }

          // finalize (interruptible)
          _ <- KyoInterOp.get {
            val (wLast, _) = lastState
            closeWriter(wLast)
          }

          // return result
          res <- KyoInterOp.get {
            val (_, totalBytes) = lastState
            CatsIO.pure(UploadResult(bytes = totalBytes, etag = None, generation = None))
          }
        yield res

      val ioResult: CatsIO[UploadResult] =
        KyoInterOp.run(ctx)(program).guaranteeCase {
          case Outcome.Canceled() => CatsIO.unit // on cancel, don't close writer → upload not finalized
          case _                  => CatsIO.unit
        }

      // IO -> F using the same Dispatcher (no implicit IORuntime required)
      implicit val runtime = cats.effect.unsafe.IORuntime.global // Or a custom one
      CEAsync[F].fromFuture(CEAsync[F].delay(ioResult.unsafeToFuture()))
    }

}

object GcsUploadSink:
  /** fake-gcs-server client (HTTP, no credentials), good for local/integration tests. */
  def clientForFakeGcs(
      host: String = sys.env.getOrElse("GCS_HOST", "http://localhost:4443"),
      project: String = sys.env.getOrElse("GCP_PROJECT", "demo")
  ): Storage =
    StorageOptions
      .newBuilder()
      .setHost(host)
      .setProjectId(project)
      .setCredentials(NoCredentials.getInstance(): Credentials)
      .build()
      .getService
