/*
 * Copyright (c) 2017-2019 The Typelevel Cats-effect Project Developers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cats
package effect
package internals

import scala.concurrent.duration._
import cats.Eval
import cats.effect.{ContextShift, ExitCode, IO, Resource, Timer}
import cats.implicits._

private[effect] object IOResourceAppPlatform {
  def main(args: Array[String], contextShift: Eval[ContextShift[IO]], timer: Eval[Timer[IO]])(
    run: List[String] => Resource[IO, ExitCode]
  ): Unit = {
    val (code, shutdownAction) = mainProcess(args, contextShift, timer)(run).map(_.code).allocated.unsafeRunSync()
    if (code == 0) {
      // Return naturally from main. This allows any non-daemon
      // threads to gracefully complete their work, and managed
      // environments to execute their own shutdown hooks.
      shutdownAction.unsafeRunTimed(30.seconds)
      ()
    } else {
      sys.exit(code)
    }
  }

  def mainProcess(args: Array[String], cs: Eval[ContextShift[IO]], timer: Eval[Timer[IO]])(
    run: List[String] => Resource[IO, ExitCode]
  ): Resource[IO, ExitCode] = {
    val _ = cs.hashCode() + timer.hashCode()
    Resource(
      run(args.toList).allocated
        .flatTap {
          case (_, shutdownAction) => installShutdownHook(shutdownAction)
        }
    )
  }

  private def installShutdownHook(shutdownAction: IO[Unit]): IO[Unit] =
    IO {
      sys.addShutdownHook {
        // Should block the thread until all finalizers are executed
        shutdownAction.unsafeRunSync()
      }
      ()
    }
}
