package vim.poc

import cats.effect.{ExitCode, IO as CatsIO, IOApp}
import cats.syntax.all.*
import fs2.io.file.{Files, Path as Fs2Path}
import vim.poc.gcs.GcsUploadSink

import scala.concurrent.duration.DurationLong

/** Runnable demo: sbt "runMain vim.poc.Main gcs gs://sample-gcs-bucket/obj.bin /path/to/file [--cancel-after-ms 1000]"
  *
  * Before running: 1) docker compose -f docker/docker-compose.yml up -d 2) bash docker/init-localstack-s3.sh
  * sample-s3-bucket
  */
object Main extends IOApp:

  def run(args: List[String]): CatsIO[ExitCode] =
    parse(args) match
      case Some(cmd) => runCmd(cmd).as(ExitCode.Success)
      case None      => usage.as(ExitCode.Error)

  private sealed trait Cmd
  private object Cmd:
    final case class GCS(dest: CloudUri, file: Fs2Path, cancelMs: Option[Long]) extends Cmd

  private def parse(as: List[String]): Option[Cmd] =
    as match
      case "gcs" :: dest :: file :: rest =>
        Some(Cmd.GCS(CloudUri.parse(dest), Fs2Path(file), parseCancel(rest)))
      case _ => None

  private def parseCancel(rest: List[String]): Option[Long] =
    rest.sliding(2, 2).collectFirst { case List("--cancel-after-ms", v) => v.toLong }

  private def usage: CatsIO[Unit] =
    CatsIO.println(
      s"""
         |Usage:
         |  runMain vim.poc.Main s3  s3://<bucket>/<key>  /path/to/file  [--cancel-after-ms 1000]
         |  runMain vim.poc.Main gcs gs://<bucket>/<key>  /path/to/file  [--cancel-after-ms 1000]
         |
         |Env knobs:
         |  READ_CHUNK_KB   (default 128)  - fs2 read chunk size
         |  GCS_CHUNK_MB    (default 8)    - GCS WriteChannel chunk size
         |  GCS_THROTTLE_MS_PER_CHUNK      - Add small delays per chunk e.g.; GCS_THROTTLE_MS_PER_CHUNK=150
         |                                   Expected: no object (since only completed resumables appear).
         |                                   If you still see it, your cancel happened after finalize;
         |                                   increase --cancel-after-ms gap and/or throttle more.
         |                                   https://cloud.google.com/storage/docs/resumable-uploads
         |
         |Prereqs:
         |  docker compose -f docker/docker-compose.yml up -d
         |  bash docker/init-localstack-s3.sh <bucket>
         |""".stripMargin
    )

  private def runCmd(cmd: Cmd): CatsIO[Unit] =
    val readChunkBytes = sys.env.get("READ_CHUNK_KB").map(_.toInt).getOrElse(128) * 1024

    cmd match
      case Cmd.GCS(dest, file, cancelMs) =>
        val gcsClient = GcsUploadSink.clientForFakeGcs(
          host = sys.env.getOrElse("GCS_HOST", "http://localhost:4443"),
          project = sys.env.getOrElse("GCP_PROJECT", "demo")
        )
        // Same compatibility trick here
        val stream = Files[CatsIO].readAll(file.toNioPath, readChunkBytes)

        // Optional test throttle to make cancellation observable
        val delayPerChunkMs = sys.env.get("GCS_THROTTLE_MS_PER_CHUNK").map(_.toLong)
        val gcsChunkSize    = sys.env.get("GCS_CHUNK_MB").map(_.toInt).getOrElse(8) * 1024 * 1024

        val throttledStream: fs2.Stream[CatsIO, Byte] =
          delayPerChunkMs match
            case Some(ms) =>
              stream
                .chunkN(gcsChunkSize, allowFewer = true)
                .evalTap(_ => CatsIO.sleep(ms.millis))
                .unchunks
            case None =>
              stream

        val sink = new vim.poc.gcs.GcsUploadSink[CatsIO](
          gcsClient,
          chunkSize = gcsChunkSize
        )

        cancelMs match
          case Some(ms) =>
            for
              fiber <- sink.upload(throttledStream, dest).start
              _     <- CatsIO.sleep(ms.millis) *> fiber.cancel
              _     <- fiber.join
              _     <- CatsIO.println(s"[GCS] Canceled after ${ms}ms; session will expire (not finalized).")
            yield ()
          case None =>
            sink.upload(throttledStream, dest).flatMap(r => CatsIO.println(s"[GCS] Uploaded bytes=${r.bytes}"))
