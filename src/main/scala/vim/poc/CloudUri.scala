package vim.poc

import java.net.URI

/** A parsed cloud URI like s3://bucket/key or gs://bucket/key. */

final case class CloudUri(scheme: String, bucket: String, key: String)

object CloudUri:
  def parse(s: String): CloudUri =
    val u = new URI(s)
    CloudUri(
      scheme = Option(u.getScheme).getOrElse(""),
      bucket = Option(u.getHost).getOrElse(""),
      key = Option(u.getPath).getOrElse("").stripPrefix("/")
    )
