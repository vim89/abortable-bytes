package vim.poc

/** Common sink API used by the POC. */

import fs2.Stream

trait CloudUploadSink[F[_]]:
  def upload(bytes: Stream[F, Byte], dest: CloudUri): F[UploadResult]
