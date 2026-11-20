export * as SessionReply from "./session-reply.js"

import { Tool } from "@opencode/schema/tool"
import { makeGlobalNode, makeLocationNode } from "@opencode/util/effect/app-node"
import { Context, Effect, Exit, Layer, Schema, Scope } from "effect"
import { randomUUID } from "node:crypto"
import { isAbsolute } from "node:path"
import { KeyedMutex } from "../effect/keyed-mutex.js"
import { Environment } from "../environment/index.js"
import { Location } from "../location.js"
import { Permission } from "../permission.js"
import type { SessionSchema } from "../session/schema.js"
import type { McpClient } from "./client.js"

export const toolName = "fabric_channel_reply"

export class Unavailable extends Schema.TaggedError<Unavailable>()("SessionReply.Unavailable", {
  message: Schema.String,
}) {}

type EventScope = {
  readonly source: string
  readonly eventId: string
  readonly replyKey: string
}

export interface Interface {
  readonly attach: (owner: SessionSchema.ID, event: EventScope | null) => Effect.Effect<string, Unavailable>
  readonly close: (owner: SessionSchema.ID, generation: string, stop: Effect.Effect<void>) => Effect.Effect<boolean>
  readonly tool: (owner: SessionSchema.ID) => Tool.Info | undefined
}

const input = Schema.Struct({ replyId: Schema.NonEmptyString, text: Schema.NonEmptyString })

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionReply") {}

type Lease = {
  readonly connection: McpClient.Connection
  live: boolean
}

type Attachment = {
  readonly generation: string
  readonly scope: Scope.Closeable
  readonly location: Location.Interface
  readonly lease: Lease | undefined
}

class Owners extends Context.Service<
  Owners,
  {
    readonly attachments: Map<SessionSchema.ID, Attachment>
    readonly locks: KeyedMutex.KeyedMutex<SessionSchema.ID>
  }
>()("@opencode/SessionReplyOwners") {}

// A session can move. Closure must still find its attachment in the previous location.
const ownersNode = makeGlobalNode({
  service: Owners,
  layer: Layer.sync(Owners, () => Owners.of({ attachments: new Map(), locks: KeyedMutex.makeUnsafe() })),
  deps: [],
})

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const root = yield* Effect.scope
    const environment = yield* Environment.Service
    const location = yield* Location.Service
    const permission = yield* Permission.Service
    const owners = yield* Owners
    const helper = process.env.OPENCODE_FABRIC_REPLY_HELPER
    const socket = process.env.OPENCODE_FABRIC_REPLY_SOCKET
    const openReply = Effect.fnUntraced(function* (owner: SessionSchema.ID, event: EventScope) {
      if (!helper || !socket || !isAbsolute(helper) || !isAbsolute(socket) || location.workspaceID)
        return yield* new Unavailable({ message: "The local Fabric reply helper is disabled" })
      if (
        !/^[0-9a-f]{64}$/i.test(event.replyKey) ||
        [event.source, event.eventId].some((value) => value.length === 0 || value.length > 255 || value.includes("\0"))
      )
        return yield* new Unavailable({ message: "Invalid Fabric reply event scope" })
      const { McpClient } = yield* Effect.promise(() => import("./client.js"))
      const connection = yield* McpClient.connect({
        server: toolName,
        directory: location.directory,
        roots: false,
        config: {
          type: "local",
          command: [helper, socket, owner, event.source, event.eventId],
          environment: { FABD_CHANNEL_REPLY_KEY: event.replyKey, FABD_CHANNEL_REPLY_OWNER_HELLO: "1" },
          protocol: "legacy",
        },
      }).pipe(Effect.mapError(() => new Unavailable({ message: "The pinned Fabric reply helper is unavailable" })))
      const lease = { connection, live: true }
      connection.onClose(() => {
        lease.live = false
      })
      const tools = yield* connection
        .tools()
        .pipe(Effect.mapError(() => new Unavailable({ message: "The pinned Fabric reply helper is unavailable" })))
      if (tools.length !== 1 || tools[0]?.name !== "reply" || !lease.live)
        return yield* new Unavailable({ message: "The pinned helper did not expose the Fabric reply tool" })
      return lease
    })

    return Service.of({
      attach: (owner, event) =>
        owners.locks.withLock(owner)(
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const scope = yield* Scope.fork(root)
              const lease =
                event === null
                  ? undefined
                  : yield* restore(
                      openReply(owner, event).pipe(
                        Scope.provide(scope),
                        Effect.provideService(Environment.Service, environment),
                      ),
                    ).pipe(
                      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, Exit.void) : Effect.void)),
                    )
              const attachment = { generation: randomUUID(), scope, location, lease }
              yield* Scope.addFinalizer(
                scope,
                Effect.sync(() => {
                  if (owners.attachments.get(owner) === attachment) owners.attachments.delete(owner)
                }),
              )
              const previous = owners.attachments.get(owner)
              owners.attachments.set(owner, attachment)
              if (previous) yield* Scope.close(previous.scope, Exit.void)
              return attachment.generation
            }),
          ),
        ),
      close: (owner, generation, stop) =>
        owners.locks.withLock(owner)(
          Effect.gen(function* () {
            const attachment = owners.attachments.get(owner)
            if (attachment?.generation !== generation) return false
            owners.attachments.delete(owner)
            // Stop work before closing the helper or admitting a successor attachment.
            yield* stop.pipe(Effect.ensuring(Scope.close(attachment.scope, Exit.void)))
            return true
          }).pipe(Effect.uninterruptible),
        ),
      tool: (owner) => {
        const attachment = owners.attachments.get(owner)
        const lease = attachment?.lease
        if (!attachment || attachment.location !== location || !lease?.live) return
        return {
          name: toolName,
          options: { codemode: false },
          description:
            "Reply to this session’s inbound Fabric channel event. Reuse the same replyId and text to recover its receipt.",
          input,
          output: Schema.Unknown,
          execute: (args: typeof input.Type, context) =>
            Effect.gen(function* () {
              yield* permission
                .assert({
                  action: toolName,
                  resources: ["*"],
                  save: ["*"],
                  metadata: {},
                  ...Permission.invocation(context),
                })
                .pipe(Effect.mapError(() => new Tool.Error({ message: "Fabric reply tool permission denied" })))
              const result = yield* lease.connection
                .callTool({
                  name: "reply",
                  args,
                  sessionID: context.sessionID,
                  canDispatch: () =>
                    context.sessionID === owner && lease.live && owners.attachments.get(owner) === attachment,
                })
                .pipe(Effect.mapError(() => new Tool.Error({ message: "The Fabric reply lease is unavailable" })))
              const content = result.content.filter((part) => part.type === "text")
              if (result.isError)
                return yield* new Tool.Error({
                  message: content.map((part) => part.text).join("\n") || "Fabric reply failed",
                })
              return { output: result.structured ?? null, content }
            }),
        }
      },
    })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Environment.node, Location.node, Permission.node, ownersNode],
})
