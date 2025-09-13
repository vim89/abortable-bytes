package vim.poc

/** Unified upload result across providers. */

final case class UploadResult(bytes: Long, etag: Option[String], generation: Option[String])
