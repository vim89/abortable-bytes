package vim.poc

import java.util.concurrent.CompletableFuture
import cats.effect.IO as CatsIO

/** Helper to bridge Java CompletableFutures into CatsIO. */

object JCF:
  inline def toIO[A](thunk: => CompletableFuture[A]): CatsIO[A] =
    CatsIO.fromCompletableFuture(CatsIO(thunk))
