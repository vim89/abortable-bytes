package vim.poc.gcs

import com.google.cloud.{NoCredentials, WriteChannel}
import kyo.<
import vim.poc.{CloudUploadSink, CloudUri, KyoInterOp, UploadResult}
import cats.arrow.FunctionK
import cats.effect.IO as CatsIO
import cats.effect.kernel.{Outcome, Async as CEAsync}
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import fs2.Stream
import com.google.auth.Credentials
import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageException, StorageOptions}

import java.io.*
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference

/** Resumable GCS upload using WriteChannel.capture/restore with FS2 + Kyo interop.
  *
  * Generic in `F`: accepts `Stream[F, Byte]` and returns `F[UploadResult]`. Requires a Cats Effect `Async[F]` (no
  * LiftIO needed).
  */
final class GcsUploadSink[F[_]](storage: Storage, chunkSize: Int = 8 * 1024 * 1024)(using
    CEAsync[F]
) extends CloudUploadSink[F]:

  // ---- capture/restore kept in-memory for demo (persist externally if you want crash-resume across processes)
  private final case class Capture(bytes: Array[Byte])
  private val captureRef = new AtomicReference[Option[Capture]](None)

  // low-level blocking helpers in CatsIO (keeps SDK blocking off F’s compute pool)
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

  private def closeWriter(w: WriteChannel): CatsIO[Unit] =
    CatsIO.interruptibleMany(w.close())

  private def writeChunk(w: WriteChannel, arr: Array[Byte]): CatsIO[Int] =
    // CatsIO.blocking(w.write(ByteBuffer.wrap(arr)))
    // attempts to cancel using Thread.interrupt if the fiber is canceled mid-write
    CatsIO.interruptibleMany(w.write(ByteBuffer.wrap(arr)))

  private def isRetryable(t: Throwable): Boolean =
    t match
      case e: StorageException =>
        val c = e.getCode
        c == 408 || c == 429 || (c >= 500 && c < 600) // typical transient codes
      case _ => false

  /** Upload a byte stream to gs://bucket/key; returns exact bytes (generation/etag not exposed on close). */
  def upload(bytes: Stream[F, Byte], dest: CloudUri): F[UploadResult] =
    val blobId   = BlobId.of(dest.bucket, dest.key)
    val blobInfo = BlobInfo.newBuilder(blobId).build()

    // Use a Dispatcher to (1) translate Stream[F, *] -> Stream[IO, *] and (2) convert IO back to F without LiftIO
    Dispatcher.parallel[F].use { dispatcher =>
      // F ~> IO (safe) using Dispatcher’s unsafeToFuture wrapped in IO.fromFuture
      val toIO: FunctionK[F, CatsIO] = new FunctionK[F, CatsIO]:
        def apply[A](fa: F[A]): CatsIO[A] =
          CatsIO.fromFuture(CatsIO(dispatcher.unsafeToFuture(fa)))

      val bytesIO: Stream[CatsIO, Byte] = bytes.translate(toIO)

      // Core program in Kyo (effect set bridged via KyoInterop.get/run)
      val program: UploadResult < KyoInterOp.BaseFX =
        KyoInterOp.get {
          def writeWithRetry(cur: WriteChannel, arr: Array[Byte], attempt: Int = 0): CatsIO[(WriteChannel, Int)] =
            writeChunk(cur, arr).attempt.flatMap {
              case Right(n) =>
                CatsIO.pure((cur, n))
              case Left(e) if isRetryable(e) && attempt < 3 =>
                val nextWriterIO =
                  captureRef.get() match
                    case Some(cap) => restoreWriter(cap)
                    case None      => newWriter(blobInfo)
                nextWriterIO.flatMap(nw => writeWithRetry(nw, arr, attempt + 1))
              case Left(e) =>
                CatsIO.raiseError(e)
            }

          for
            w0 <- newWriter(blobInfo)

            // Keep (writer,total) as state; emit the same so we can grab the last element easily.
            lastState <- bytesIO
              .chunkN(chunkSize, allowFewer = true)
              .evalMapAccumulate((w0, 0L)) { case ((w, total), chunk) =>
                val arr = chunk.toArray
                for
                  (w2, written) <- writeWithRetry(w, arr)
                  nextTotal = total + written.toLong
                  _ <- if ((nextTotal / chunkSize) % 16 == 0) captureWriter(w2) else CatsIO.unit
                yield ((w2, nextTotal), (w2, nextTotal)) // (newState, emittedValue)
              }
              .map(_._2) // stream of (writer,total)
              .compile
              .lastOrError // final (writer,total)

            (wLast, totalBytes) = lastState
            _ <- CatsIO.interruptibleMany(wLast.close()) // finalize upload (interruptible)
          yield UploadResult(bytes = totalBytes, etag = None, generation = None)
        }

      val ioResult: CatsIO[UploadResult] = {
        KyoInterOp.run(program).guaranteeCase {
          case Outcome.Canceled() => CatsIO.unit // on cancel, don't close writer → upload not finalized
          case _                  => CatsIO.unit
        }
      }

      CEAsync[F].fromFuture(CEAsync[F].delay(ioResult.unsafeToFuture()))
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
