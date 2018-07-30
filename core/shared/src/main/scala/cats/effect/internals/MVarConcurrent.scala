/*
 * Copyright (c) 2017-2018 The Typelevel Cats-effect Project Developers
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

package cats.effect
package internals

import java.util.concurrent.atomic.AtomicReference
import cats.effect.concurrent.MVar
import cats.effect.internals.Callback.rightUnit
import scala.annotation.tailrec

/**
 * [[MVar]] implementation for [[Concurrent]] data types.
 */
private[effect] final class MVarConcurrent[F[_], A] private (
  initial: MVarConcurrent.State[A])(implicit F: Concurrent[F])
  extends MVar[F, A] {

  import MVarConcurrent._

  /** Shared mutable state. */
  private[this] val stateRef = new AtomicReference[State[A]](initial)

  def put(a: A): F[Unit] =
    F.cancelable(unsafePut(a))

  def tryPut(a: A): F[Boolean] =
    F.async(unsafePut1(a))

  def take: F[A] =
    F.cancelable(unsafeTake)

  def tryTake: F[Option[A]] =
    F.async(unsafeTake1)

  def read: F[A] =
    F.cancelable(unsafeRead)

  def isEmpty: F[Boolean] =
    F.delay {
      stateRef.get match {
        case WaitForPut(_, _) => true
        case WaitForTake(_, _) => false
      }
    }


  @tailrec
  private def unsafePut1(a: A)(onPut: Listener[Boolean]): Unit = {
    stateRef.get match {
      case WaitForTake(_, _) => onPut(Right(false))

      case current @ WaitForPut(reads, takes) =>
        var first: Listener[A] = null
        val update: State[A] =
          if (takes.isEmpty) State(a) else {
            val (x, rest) = takes.dequeue
            first = x
            if (rest.isEmpty) State.empty[A]
            else WaitForPut(LinkedMap.empty, rest)
          }

        if (!stateRef.compareAndSet(current, update)) {
          unsafePut1(a)(onPut) // retry
        } else {
          val value = Right(a)
          // Satisfies all current `read` requests found
          streamAll(value, reads)
          // Satisfies the first `take` request found
          if (first ne null) first(value)
          // Signals completion of `put`
          onPut(Right(true))
        }
    }
  }

  @tailrec private def unsafePut(a: A)(onPut: Listener[Unit]): F[Unit] = {
    stateRef.get match {
      case current @ WaitForTake(value, listeners) =>
        val id = new Id
        val newMap = listeners.updated(id, (a, onPut))
        val update = WaitForTake(value, newMap)

        if (stateRef.compareAndSet(current, update)) {
          F.delay(unsafeCancelPut(id))
        } else {
          unsafePut(a)(onPut) // retry
        }

      case current @ WaitForPut(reads, takes) =>
        var first: Listener[A] = null
        val update: State[A] =
          if (takes.isEmpty) State(a) else {
            val (x, rest) = takes.dequeue
            first = x
            if (rest.isEmpty) State.empty[A]
            else WaitForPut(LinkedMap.empty, rest)
          }

        if (stateRef.compareAndSet(current, update)) {
          val value = Right(a)
          // Satisfies all current `read` requests found
          streamAll(value, reads)
          // Satisfies the first `take` request found
          if (first ne null) first(value)
          // Signals completion of `put`
          onPut(rightUnit)
          F.unit
        } else {
          unsafePut(a)(onPut) // retry
        }
    }
  }

  // Impure function meant to cancel the put request
  @tailrec private def unsafeCancelPut(id: Id): Unit =
    stateRef.get() match {
      case current @ WaitForTake(_, listeners) =>
        val update = current.copy(listeners = listeners - id)
        if (!stateRef.compareAndSet(current, update)) {
          unsafeCancelPut(id) // retry
        }
      case _ =>
        ()
    }

  @tailrec
  private def unsafeTake1(onTake: Listener[Option[A]]): Unit = {
    val current: State[A] = stateRef.get
    current match {
      case WaitForTake(value, queue) =>
        if (queue.isEmpty) {
          if (stateRef.compareAndSet(current, State.empty))
            // Signals completion of `take`
            onTake(Right(Some(value)))
          else {
            unsafeTake1(onTake) // retry
          }
        } else {
          val ((ax, notify), xs) = queue.dequeue
          val update = WaitForTake(ax, xs)
          if (stateRef.compareAndSet(current, update)) {
            // Signals completion of `take`
            onTake(Right(Some(value)))
            // Complete the `put` request waiting on a notification
            notify(rightUnit)
          } else {
            unsafeTake1(onTake) // retry
          }
        }

      case WaitForPut(_, _) =>
        onTake(Right(None))
    }
  }

  @tailrec
  private def unsafeTake(onTake: Listener[A]): F[Unit] = {
    stateRef.get match {
      case current @ WaitForTake(value, queue) =>
        if (queue.isEmpty) {
          if (stateRef.compareAndSet(current, State.empty)) {
            onTake(Right(value))
            F.unit
          } else {
            unsafeTake(onTake) // retry
          }
        } else {
          val ((ax, notify), xs) = queue.dequeue
          if (stateRef.compareAndSet(current, WaitForTake(ax, xs))) {
            onTake(Right(value))
            notify(rightUnit)
            F.unit
          } else {
            unsafeTake(onTake) // retry
          }
        }

      case current @ WaitForPut(reads, takes) =>
        val id = new Id
        val newQueue = takes.updated(id, onTake)
        if (stateRef.compareAndSet(current, WaitForPut(reads, newQueue)))
          F.delay(unsafeCancelTake(id))
        else {
          unsafeTake(onTake) // retry
        }
    }
  }

  @tailrec private def unsafeCancelTake(id: Id): Unit =
    stateRef.get() match {
      case current @ WaitForPut(reads, takes) =>
        val newMap = takes - id
        val update: State[A] = WaitForPut(reads, newMap)
        if (!stateRef.compareAndSet(current, update)) {
          unsafeCancelTake(id)
        }
      case _ =>
    }


  @tailrec
  private def unsafeRead(onRead: Listener[A]): F[Unit] = {
    val current: State[A] = stateRef.get
    current match {
      case WaitForTake(value, _) =>
        // A value is available, so complete `read` immediately without
        // changing the sate
        onRead(Right(value))
        F.unit

      case WaitForPut(reads, takes) =>
        // No value available, enqueue the callback
        val id = new Id
        val newQueue = reads.updated(id, onRead)
        if (stateRef.compareAndSet(current, WaitForPut(newQueue, takes)))
          F.delay(unsafeCancelRead(id))
        else {
          unsafeRead(onRead) // retry
        }
    }
  }

  private def unsafeCancelRead(id: Id): Unit =
    stateRef.get() match {
      case current @ WaitForPut(reads, takes) =>
        val newMap = reads - id
        val update: State[A] = WaitForPut(newMap, takes)
        if (!stateRef.compareAndSet(current, update)) {
          unsafeCancelRead(id)
        }
      case _ => ()
    }

  // For streaming a value to a whole `reads` collection
  private def streamAll(value: Either[Nothing, A], listeners: LinkedMap[Id, Listener[A]]): Unit = {
    val cursor = listeners.values.iterator
    while (cursor.hasNext)
      cursor.next().apply(value)
  }
}

private[effect] object MVarConcurrent {
  /** Builds an [[MVarConcurrent]] instance with an `initial` value. */
  def apply[F[_], A](initial: A)(implicit F: Concurrent[F]): MVar[F, A] =
    new MVarConcurrent[F, A](State(initial))

  /** Returns an empty [[MVarConcurrent]] instance. */
  def empty[F[_], A](implicit F: Concurrent[F]): MVar[F, A] =
    new MVarConcurrent[F, A](State.empty)

  /**
   * Internal API — Matches the callback type in `cats.effect.Async`,
   * but we don't care about the error.
   */
  private type Listener[-A] = Either[Nothing, A] => Unit

  /** Used with [[LinkedMap]] to identify callbacks that need to be cancelled. */
  private final class Id extends Serializable

  /** ADT modelling the internal state of `MVar`. */
  private sealed trait State[A]

  /** Private [[State]] builders.*/
  private object State {
    private[this] val ref = WaitForPut[Any](LinkedMap.empty, LinkedMap.empty)
    def apply[A](a: A): State[A] = WaitForTake(a, LinkedMap.empty)
    /** `Empty` state, reusing the same instance. */
    def empty[A]: State[A] = ref.asInstanceOf[State[A]]
  }

  /**
   * `MVarConcurrent` state signaling it has `take` callbacks
   * registered and we are waiting for one or multiple
   * `put` operations.
   */
  private final case class WaitForPut[A](
    reads: LinkedMap[Id, Listener[A]], takes: LinkedMap[Id, Listener[A]])
    extends State[A]

  /**
   * `MVarConcurrent` state signaling it has one or more values enqueued,
   * to be signaled on the next `take`.
   *
   * @param value is the first value to signal
   * @param listeners are the rest of the `put` requests, along with the
   *        callbacks that need to be called whenever the corresponding
   *        value is first in line (i.e. when the corresponding `put`
   *        is unblocked from the user's point of view)
   */
  private final case class WaitForTake[A](
    value: A, listeners: LinkedMap[Id, (A, Listener[Unit])])
    extends State[A]
}

