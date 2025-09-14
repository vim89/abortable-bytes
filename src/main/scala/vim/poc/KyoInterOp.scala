package vim.poc

import cats.effect.IO as CatsIO
import kyo.{Cats as KyoCats, Async as KyoAsync, Abort, Env}
import kyo.<

/** Kyo <-> Cats bridge + typed upload context Demonstrates multiple effects tracked via intersection types: BaseFX =
  * Async & Abort[Throwable] FX = BaseFX & Env[UploadCtx]
  */
object KyoInterOp {

  /** Typed context we want available during uploads (for logging/audit/headers). */
  final case class UploadCtx(traceId: String, auditId: String)

  /** Base effects that Cats interop can run directly. */
  type BaseFX = KyoAsync & Abort[Throwable]

  /** Full effect row we use inside programs (adds typed Env). */
  type FX = BaseFX & Env[UploadCtx]

  /** Run a Kyo program after the Env has been provided (eliminates Env, then runs BaseFX to Cats IO). */
  inline def run[A](ctx: UploadCtx)(ka: A < FX): CatsIO[A] =
    KyoCats.run(Env.run(ctx)(ka))

  /** Run a Kyo program as Cats IO (CE cancellation cooperates). */
  inline def run[A](ka: A < BaseFX): CatsIO[A] = KyoCats.run(ka)

  /** Lift Cats IO into the Kyo BaseFX row. Safe to compose with additional effects (e.g., Env) in for-comps. */
  inline def get[A](ioa: CatsIO[A]): A < BaseFX = KyoCats.get(ioa)
}
