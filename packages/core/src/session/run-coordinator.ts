export * as SessionRunCoordinator from "./run-coordinator.js"

import { Deferred, Effect, Exit, Fiber, FiberSet, Option, Scope } from "effect"
import type { Promotable } from "./inbox.js"

/** Serializes execution for each key while allowing different keys to run concurrently. */
export interface Coordinator<Key, E, Reason = never> {
  /** Snapshots keys with an execution owned by this coordinator. */
  readonly active: Effect.Effect<ReadonlySet<Key>>
  /** Snapshots keys genuinely draining: owned and not stopping. Settlement windows are excluded. */
  readonly draining: Effect.Effect<ReadonlySet<Key>>
  /** Identifies the exact draining execution. Changes on a successor or process restart. */
  readonly executionID: (key: Key) => Effect.Effect<string | undefined>
  /**
   * Stages an operation for this execution's own drain boundary. Idle, stopping,
   * mismatched and already-settling executions refuse it without running it.
   */
  readonly stage: <A, Failure>(
    key: Key,
    expected: string,
    operation: Effect.Effect<StagedResult<A>, Failure>,
  ) => Effect.Effect<Option.Option<A>, Failure>
  /** Checks ownership for one key, including cleanup and terminal settlement. */
  readonly isActive: (key: Key) => Effect.Effect<boolean>
  /** Starts an execution while idle, or joins the active execution and returns its exit. */
  readonly run: (key: Key) => Effect.Effect<void, E>
  /** Rings the doorbell: an idle key starts an execution; an active one drains again before settling. */
  readonly wake: (key: Key, scope?: Promotable) => Effect.Effect<void>
  /**
   * Stops the active execution and clears its doorbell. No-op when idle. Resolves once the
   * interruption is accepted, not when cleanup settles: the execution fiber finishes its
   * finalizers and settled hook on its own time. Returns whether an active execution was
   * interrupted.
   */
  readonly interrupt: (key: Key, reason?: Reason) => Effect.Effect<boolean>
  /** Why the key's execution is being stopped, once `interrupt` stopped it; undefined for teardown or when running. */
  readonly interruption: (key: Key) => Reason | undefined
  /** Resolves once no execution is active for the key. Returns immediately when already idle and never starts work. */
  readonly awaitIdle: (key: Key) => Effect.Effect<void>
}

/**
 * One execution is a busy period for one key: one fiber that drains from the first wake
 * until the key would stay idle. `pendingWake` is the doorbell: work recorded during the
 * execution rings it with the scope that work needs, and the execution loop drains again
 * instead of ending. The doorbell closes the gap between a drain's last eligibility check
 * and the idle transition, since those cannot be one atomic step. `done` resolves joiners
 * with this execution's exit.
 */
type Execution<E, Reason> = {
  readonly id: string
  readonly done: Deferred.Deferred<void, E>
  readonly staged: Array<{ run: Effect.Effect<boolean>; refuse: () => void }>
  owner?: Fiber.Fiber<void>
  scope: Promotable
  pendingWake?: Promotable
  open: boolean
  stopping: boolean
  interruptionReason?: Reason
}

export interface DrainBoundary {
  readonly hasStaged: () => boolean
  readonly flush: Effect.Effect<boolean>
}

export type StagedResult<A> = { readonly value: A; readonly changed: boolean }

/**
 * ```text
 *              wake | run
 *      idle ──────────────▶ execution (one fiber)
 *                             drain ⟲ doorbell rung mid-drain
 *                             │ exit (settled hook runs)
 *      doorbell quiet ◀───────┴───────▶ doorbell rung
 *      idle, waiters get exit          successor execution,
 *                                      waiters get this exit
 * ```
 */
export const make = <Key, E, Reason = never>(options: {
  readonly drain: (key: Key, force: boolean, scope: Promotable, boundary: DrainBoundary) => Effect.Effect<void, E>
  /** Runs once when a process-local busy period begins, before its first drain. */
  readonly started?: (key: Key) => Effect.Effect<void>
  /**
   * Runs in the execution fiber for every exit, including interruption, after the final
   * drain and before the execution settles (waiters resolve after it completes).
   */
  readonly settled?: (key: Key, exit: Exit.Exit<void, E>, reason?: Reason) => Effect.Effect<void>
}): Effect.Effect<Coordinator<Key, E, Reason>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const executions = new Map<Key, Execution<E, Reason>>()
    const fork = yield* FiberSet.makeRuntime<never, void, never>()

    const boundary = (execution: Execution<E, Reason>): DrainBoundary => ({
      hasStaged: () => execution.staged.length !== 0,
      flush: Effect.uninterruptible(
        Effect.suspend(() => {
          if (!execution.open || execution.stopping) return Effect.succeed(false)
          const staged = execution.staged.splice(0)
          return Effect.forEach(staged, (operation) =>
            Effect.suspend(() =>
              execution.stopping
                ? Effect.sync(() => {
                    operation.refuse()
                    return false
                  })
                : operation.run,
            ),
          ).pipe(Effect.map((results) => results.some(Boolean)))
        }),
      ),
    })

    const loop = (key: Key, execution: Execution<E, Reason>, force: boolean): Effect.Effect<void, E> =>
      Effect.suspend(() => options.drain(key, force, execution.scope, boundary(execution))).pipe(
        Effect.andThen(
          Effect.suspend(() => {
            if (execution.stopping) return Effect.void
            if (execution.pendingWake === undefined && execution.staged.length === 0) {
              execution.open = false
              return Effect.void
            }
            execution.scope = execution.pendingWake ?? "steer"
            execution.pendingWake = undefined
            // Trampoline so drains that complete synchronously cannot grow the stack.
            return Effect.yieldNow.pipe(Effect.andThen(loop(key, execution, false)))
          }),
        ),
      )

    const start = (key: Key, force: boolean, scope: Promotable) => {
      const execution: Execution<E, Reason> = {
        id: crypto.randomUUID(),
        done: Deferred.makeUnsafe<void, E>(),
        staged: [],
        scope,
        open: false,
        stopping: false,
      }
      executions.set(key, execution)
      // The leading yield lets `owner` be assigned before the drain can settle, and keeps
      // failing self-waking executions from growing the stack across successor starts.
      // Drains start one tick after wake; callers observe progress through events or run.
      execution.owner = fork(
        Effect.yieldNow.pipe(
          Effect.andThen(Effect.uninterruptible(options.started?.(key) ?? Effect.void)),
          Effect.andThen(
            Effect.sync(() => {
              execution.open = true
            }),
          ),
          Effect.andThen(loop(key, execution, force)),
          Effect.onExit((exit) =>
            Effect.sync(() => {
              execution.open = false
              execution.owner = undefined
              execution.staged.splice(0).forEach((operation) => operation.refuse())
            }).pipe(Effect.andThen(options.settled?.(key, exit, execution.interruptionReason) ?? Effect.void)),
          ),
          Effect.onExit((exit) => Effect.sync(() => settle(key, execution, exit))),
          Effect.exit,
          Effect.asVoid,
        ),
      )
      return execution
    }

    // A doorbell that survives the execution loop (rung after the loop decided to end, or
    // during failure or interruption cleanup) starts a fresh execution for the remaining work.
    const settle = (key: Key, execution: Execution<E, Reason>, exit: Exit.Exit<void, E>) => {
      if (execution.pendingWake) start(key, false, execution.pendingWake)
      else executions.delete(key)
      Deferred.doneUnsafe(execution.done, exit)
    }

    const isActive = (key: Key) => Effect.sync(() => executions.has(key))
    const accepting = (key: Key) => {
      const execution = executions.get(key)
      return execution?.open && !execution.stopping ? execution : undefined
    }
    const executionID = (key: Key) => Effect.sync(() => accepting(key)?.id)
    const stage = <A, Failure>(key: Key, expected: string, operation: Effect.Effect<StagedResult<A>, Failure>) =>
      Effect.suspend(() => {
        const execution = accepting(key)
        if (execution?.id !== expected) return Effect.succeed(Option.none<A>())
        const result = Deferred.makeUnsafe<Option.Option<A>, Failure>()
        execution.staged.push({
          run: Effect.exit(operation).pipe(
            Effect.map((exit) => {
              Deferred.doneUnsafe(
                result,
                Exit.map(exit, (outcome) => Option.some(outcome.value)),
              )
              return Exit.isSuccess(exit) && exit.value.changed
            }),
          ),
          refuse: () => Deferred.doneUnsafe(result, Exit.succeed(Option.none())),
        })
        return Deferred.await(result)
      })

    const run = (key: Key): Effect.Effect<void, E> =>
      Effect.suspend(() => {
        const execution = executions.get(key)
        if (execution !== undefined) {
          // A stopping execution refuses joiners: wait out its cleanup, then run fresh.
          if (execution.stopping)
            return Deferred.await(execution.done).pipe(Effect.ignoreCause, Effect.andThen(run(key)))
          return Deferred.await(execution.done)
        }
        return Deferred.await(start(key, true, "input").done)
      })

    const wake = (key: Key, scope: Promotable = "input") =>
      Effect.sync(() => {
        const execution = executions.get(key)
        if (execution !== undefined) {
          // Coalesced wakes keep the widest scope: "input" subsumes "steer".
          execution.pendingWake = execution.pendingWake === "input" ? "input" : scope
          return
        }
        start(key, false, scope)
      })

    const interrupt = (key: Key, reason?: Reason): Effect.Effect<boolean> =>
      Effect.sync(() => {
        const execution = executions.get(key)
        if (execution === undefined || execution.stopping) return false
        if (execution.owner === undefined) {
          // Settlement window: the owner exited but the settled hook has not finished. The
          // terminal outcome is already decided, so no reason attaches — but the interrupt
          // still claims the recorded wakes so settle does not start a dead-intent successor.
          execution.pendingWake = undefined
          return false
        }
        execution.stopping = true
        // Wakes recorded so far belong to the interrupted intent; the interrupt claims them.
        // Wakes arriving during cleanup are new admissions and restart normally at settle.
        execution.pendingWake = undefined
        execution.interruptionReason = reason
        // Fire and forget: nobody benefits from waiting out cleanup here, and callers like
        // the interrupt endpoint must acknowledge immediately even when finalizers are slow.
        fork(Fiber.interrupt(execution.owner))
        return true
      })

    // One execution's `done` already spans coalesced continuations; re-check after it
    // settles to cover a successor execution started by a late doorbell.
    const awaitIdle = (key: Key): Effect.Effect<void> =>
      Effect.suspend(() => {
        const execution = executions.get(key)
        if (execution === undefined) return Effect.void
        return Deferred.await(execution.done).pipe(Effect.ignoreCause, Effect.andThen(awaitIdle(key)))
      })

    const interruption = (key: Key) => executions.get(key)?.interruptionReason

    const draining = Effect.sync(
      () =>
        new Set(
          [...executions.entries()].flatMap(([key, execution]) =>
            execution.owner !== undefined && !execution.stopping ? [key] : [],
          ),
        ),
    )

    return {
      active: Effect.sync(() => new Set(executions.keys())),
      draining,
      executionID,
      stage,
      isActive,
      run,
      wake,
      interrupt,
      interruption,
      awaitIdle,
    }
  })
