import assert from "node:assert/strict"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"

const binary = path.join(import.meta.dir, "../dist", `cli-${process.platform}-${process.arch}`, "bin/opencode")
const home = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "opencode-active-steer-")))
const state = path.join(home, "state", "opencode")
const processes: Array<ReturnType<typeof Bun.spawn>> = []
const turns: string[] = []
const releaseFirst = Promise.withResolvers<void>()
const releaseNext = Promise.withResolvers<void>()
const abortEvents = new AbortController()
const seen: Array<{ type: string; data: { sessionID?: string; inboxID?: string; executionId?: string } }> = []
const encoder = new TextEncoder()

const stub = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  async fetch(request) {
    if (request.method === "GET") return Response.json({ data: [{ id: "echo", object: "model" }] })
    const body = (await request.json()) as {
      messages: { role: string; content: string | { text?: string }[] }[]
      model: string
    }
    const user = body.messages.findLast((message) => message.role === "user")?.content
    const text = Array.isArray(user) ? user.map((part) => part.text ?? "").join("\n") : (user ?? "")
    const naming = body.messages.some(
      (message) => message.role === "system" && String(message.content).startsWith("You name conversations"),
    )
    if (!naming) turns.push(text)
    const gate =
      !naming && (text === "initial" ? releaseFirst.promise : text === "successor" ? releaseNext.promise : undefined)
    return new Response(
      new ReadableStream({
        async start(stream) {
          const event = (delta: object, finish_reason: string | null, usage?: object) =>
            stream.enqueue(
              encoder.encode(
                `data: ${JSON.stringify({ id: "stub", object: "chat.completion.chunk", created: 0, model: body.model, choices: [{ index: 0, delta, finish_reason }], usage })}\n\n`,
              ),
            )
          event({ role: "assistant", content: "" }, null)
          await gate
          event({ content: `echo: ${text}` }, null)
          event({}, "stop", { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 })
          stream.enqueue(encoder.encode("data: [DONE]\n\n"))
          stream.close()
        },
      }),
      { headers: { "content-type": "text/event-stream" } },
    )
  },
})

let service: { url: string; password: string; pid: number } | undefined
async function start() {
  const child = Bun.spawn([binary, "serve", "--service", "--hostname", "127.0.0.1", "--port", "0"], {
    cwd: home,
    env: {
      HOME: home,
      PATH: process.env.PATH,
      OPENCODE_TEST_HOME: home,
      OPENCODE_DB: path.join(home, "service.db"),
      XDG_CONFIG_HOME: path.join(home, "config"),
      XDG_DATA_HOME: path.join(home, "data"),
      XDG_CACHE_HOME: path.join(home, "cache"),
      XDG_STATE_HOME: path.join(home, "state"),
    },
    stdout: "ignore",
    stderr: "inherit",
  })
  processes.push(child)
  await waitFor(async () => {
    if (child.exitCode !== null) throw new Error(`OpenCode exited ${child.exitCode}`)
    const files = await fs.readdir(state).catch(() => [])
    const registration = files.find((file) => /^service(?:-[\w.-]+)?\.json$/.test(file))
    if (!registration) return false
    const current = (await Bun.file(path.join(state, registration))
      .json()
      .catch(() => undefined)) as typeof service
    if (current?.pid !== child.pid) return false
    service = current
    return request("/api/info").then(
      (reply) => reply.status === 200,
      () => false,
    )
  }, "registered OpenCode")
  await waitFor(async () => (await request("/api/model")).status === 200, "model readiness")
  return child
}

async function request(route: string, data?: object) {
  assert.ok(service)
  const response = await fetch(new URL(route, service.url), {
    method: data === undefined ? "GET" : "POST",
    headers: {
      authorization: "Basic " + btoa(`opencode:${service.password}`),
      "content-type": "application/json",
    },
    body: data === undefined ? undefined : JSON.stringify(data),
    signal: AbortSignal.timeout(10_000),
  })
  return { status: response.status, body: await response.json() }
}

async function waitFor(condition: () => boolean | Promise<boolean>, description: string) {
  for (let attempt = 0; attempt < 400; attempt++) {
    if (await condition()) return
    await Bun.sleep(25)
  }
  throw new Error(`Timed out waiting for ${description}`)
}

try {
  await fs.mkdir(path.join(home, "config", "opencode"), { recursive: true })
  await Bun.write(
    path.join(home, "config", "opencode", "opencode.json"),
    JSON.stringify({
      provider: {
        stub: {
          npm: "@ai-sdk/openai-compatible",
          name: "Stub",
          options: { baseURL: `http://127.0.0.1:${stub.port}/v1`, apiKey: "none" },
          models: { echo: { name: "Echo" } },
        },
      },
      model: "stub/echo",
    }),
  )
  const first = await start()
  const created = await request("/api/session", { location: { directory: home } })
  assert.equal(created.status, 200)
  const session = created.body.data.id
  assert.ok(service)
  const sse = await fetch(new URL("/api/event", service.url), {
    headers: { authorization: "Basic " + btoa(`opencode:${service.password}`) },
    signal: abortEvents.signal,
  })
  assert.equal(sse.status, 200)
  const sseTask = (async () => {
    let partial = ""
    for await (const chunk of sse.body!.pipeThrough(new TextDecoderStream())) {
      const lines = (partial + chunk).split("\n")
      partial = lines.pop()!
      for (const line of lines) {
        if (line.startsWith("data: ")) seen.push(JSON.parse(line.slice(6)))
      }
    }
  })().catch((cause) => {
    if (!abortEvents.signal.aborted) throw cause
  })

  const route = `/api/experimental/session/${session}/active-steer`
  const ordinary = `/api/session/${session}/prompt`
  const active = async () => (await request(route)).body.data.executionId
  const status = async () => (await request("/api/session/active")).body.data[session]
  const inbox = async () => (await request(`/api/session/${session}/inbox`)).body.data
  const steer = { id: "msg_channel", text: "channel", expectedExecutionId: "idle" }
  assert.equal((await request(route, steer)).status, 409)
  assert.deepEqual(await inbox(), [])
  assert.deepEqual(turns, [])

  assert.equal((await request(ordinary, { text: "initial", delivery: "queue" })).status, 200)
  await waitFor(() => turns.includes("initial"), "held initial model step")
  const expectedExecutionId = await active()
  assert.ok(expectedExecutionId)
  assert.equal((await request(route, steer)).status, 409)
  const accepted = { ...steer, expectedExecutionId }
  const concurrent = [request(route, accepted), request(route, accepted)]
  assert.deepEqual(await inbox(), [])
  releaseFirst.resolve()
  for (const reply of await Promise.all(concurrent)) {
    assert.equal(reply.status, 200, JSON.stringify(reply.body))
    assert.equal(reply.body.data.id, steer.id)
  }
  await waitFor(async () => (await status()) === undefined, "first execution settlement")
  await waitFor(
    () => seen.some((event) => event.data?.sessionID === session && event.type === "session.execution.succeeded"),
    "first terminal event",
  )
  assert.deepEqual(turns, ["initial", "channel"])
  assert.equal(
    seen.filter((event) => event.data?.sessionID === session && event.type === "session.execution.started").length,
    1,
  )
  const delivery = seen.filter((event) => event.type === "session.inbox.delivered" && event.data.inboxID === steer.id)
  assert.equal(delivery.length, 1)
  assert.equal(delivery[0].data.executionId, expectedExecutionId)
  assert.deepEqual(await inbox(), [])

  const stale = { ...accepted, id: "msg_expired", text: "stale" }
  assert.equal((await request(route, stale)).status, 409)
  assert.equal((await request(route, accepted)).status, 200)
  assert.deepEqual(await inbox(), [])
  assert.deepEqual(turns, ["initial", "channel"])
  abortEvents.abort()
  await sseTask
  first.kill()
  await first.exited

  await start()
  assert.equal((await request(route, accepted)).body.data.id, steer.id)
  assert.equal((await request(ordinary, { text: "successor", delivery: "queue" })).status, 200)
  await waitFor(() => turns.includes("successor"), "held successor model step")
  assert.notEqual(await active(), expectedExecutionId)
  assert.equal((await request(route, stale)).status, 409)
  releaseNext.resolve()
  await waitFor(async () => (await status()) === undefined, "successor settlement")
  assert.deepEqual(await inbox(), [])
  assert.deepEqual(turns, ["initial", "channel", "successor"])
  console.log("OpenCode retained one native steer across restart and refused stale input in a successor")
} finally {
  abortEvents.abort()
  releaseFirst.resolve()
  releaseNext.resolve()
  stub.stop(true)
  processes.forEach((process) => process.kill())
  await Promise.all(processes.map((process) => process.exited))
  await fs.rm(home, { recursive: true, force: true })
}
