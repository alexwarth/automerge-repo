import assert from "assert"
import { describe, it } from "vitest"
import { Repo } from "../src/Repo.js"
import { pause } from "../src/helpers/pause.js"
import { PeerId } from "../src/index.js"
import {
  EphemeralMessage,
  isEphemeralMessage,
} from "../src/network/messages.js"
import connectRepos from "./helpers/connectRepos.js"
import { TestDoc } from "./types.js"

describe("ephemeral messages", () => {
  describe("relay through a sync server", () => {
    // Topology: alice <-> server <-> dan, server configured like
    // examples/sync-server (sharePolicy: async () => false), so every peer
    // on the server is in the "share" state and the relay gates in
    // DocSynchronizer apply.
    const setup = async () => {
      const alice = new Repo({ peerId: "alice" as PeerId })
      const server = new Repo({
        peerId: "server" as PeerId,
        sharePolicy: async () => false, // same as examples/sync-server
      })
      const dan = new Repo({ peerId: "dan" as PeerId })

      await connectRepos(alice, server)
      await connectRepos(dan, server)

      return { alice, server, dan }
    }

    it("relays both directions in steady state", async () => {
      const { alice, dan } = await setup()

      const aliceHandle = alice.create<TestDoc>({ foo: "bar" })
      const danHandle = await dan.find<TestDoc>(aliceHandle.url)
      await pause(50)

      const gotAtDan: unknown[] = []
      const gotAtAlice: unknown[] = []
      danHandle.on("ephemeral-message", p => gotAtDan.push(p.message))
      aliceHandle.on("ephemeral-message", p => gotAtAlice.push(p.message))

      aliceHandle.broadcast({ from: "alice" })
      danHandle.broadcast({ from: "dan" })
      await pause(50)

      assert.deepStrictEqual(gotAtDan, [{ from: "alice" }])
      assert.deepStrictEqual(gotAtAlice, [{ from: "dan" }])
    })

    it("recovers via ephemeral-only traffic after the server evicts the doc", async () => {
      // Regression test: the server relays ephemeral messages to a peer
      // only once that peer has interacted with the document. Previously
      // only sync/request messages counted as interaction, so when the
      // server lost its per-peer doc state (eviction, reconnect) while the
      // clients believed they were fully synced, ephemeral-only traffic
      // (presence, cursors, drags) could never restore delivery — leaving
      // messages flowing in one direction but not the other, indefinitely.
      // Ephemeral messages from a directly-connected peer now count as
      // interaction, so this heals on its own.
      const { alice, server, dan } = await setup()

      const aliceHandle = alice.create<TestDoc>({ foo: "bar" })
      const danHandle = await dan.find<TestDoc>(aliceHandle.url)
      await pause(50)

      // Server evicts the doc (e.g. memory management). Clients are unaware.
      await server.removeFromCache(aliceHandle.documentId)
      await pause(10)

      const gotAtDan: unknown[] = []
      const gotAtAlice: unknown[] = []
      danHandle.on("ephemeral-message", p => gotAtDan.push(p.message))
      aliceHandle.on("ephemeral-message", p => gotAtAlice.push(p.message))

      // Both sides send ephemeral-only traffic; no doc changes. The first
      // messages may be dropped while the server re-establishes peer state
      // (ephemeral delivery is best-effort), but traffic alone must be
      // enough to restore relay in both directions.
      aliceHandle.broadcast({ from: "alice", n: 1 })
      danHandle.broadcast({ from: "dan", n: 1 })
      await pause(100)
      aliceHandle.broadcast({ from: "alice", n: 2 })
      danHandle.broadcast({ from: "dan", n: 2 })
      await pause(100)

      assert.deepStrictEqual(
        gotAtDan.at(-1),
        { from: "alice", n: 2 },
        "alice -> dan should recover"
      )
      assert.deepStrictEqual(
        gotAtAlice.at(-1),
        { from: "dan", n: 2 },
        "dan -> alice should recover"
      )
    })
  })

  describe("broadcast stamping", () => {
    it("stamps every copy of one broadcast with the same session and count", async () => {
      // Receivers deduplicate ephemeral messages on (senderId, sessionId,
      // count), so all per-peer copies of one broadcast must share a stamp.
      // Previously each copy was stamped as it was sent (one count per
      // copy), so the same broadcast could be delivered twice — or an older
      // broadcast dropped entirely — when copies travel different paths.
      const alice = new Repo({ peerId: "alice" as PeerId })
      const bob = new Repo({ peerId: "bob" as PeerId })
      const charlie = new Repo({ peerId: "charlie" as PeerId })
      await connectRepos(alice, bob)
      await connectRepos(alice, charlie)

      const aliceHandle = alice.create<TestDoc>({ foo: "bar" })
      await bob.find<TestDoc>(aliceHandle.url)
      await charlie.find<TestDoc>(aliceHandle.url)
      await pause(50)

      const atBob: EphemeralMessage[] = []
      const atCharlie: EphemeralMessage[] = []
      bob.networkSubsystem.on("message", m => {
        if (isEphemeralMessage(m)) atBob.push(m)
      })
      charlie.networkSubsystem.on("message", m => {
        if (isEphemeralMessage(m)) atCharlie.push(m)
      })

      aliceHandle.broadcast({ hello: "everyone" })
      await pause(50)

      assert.strictEqual(atBob.length, 1)
      assert.strictEqual(atCharlie.length, 1)
      assert.strictEqual(atBob[0].senderId, "alice")
      assert.strictEqual(atBob[0].sessionId, atCharlie[0].sessionId)
      assert.strictEqual(atBob[0].count, atCharlie[0].count)
    })

    it("delivers a broadcast exactly once per peer in a mesh", async () => {
      // Fully-connected triangle: each of bob and charlie receives alice's
      // broadcast directly AND relayed by the other. With a shared stamp
      // the relayed copy is recognized as a duplicate and suppressed.
      const alice = new Repo({ peerId: "alice" as PeerId })
      const bob = new Repo({ peerId: "bob" as PeerId })
      const charlie = new Repo({ peerId: "charlie" as PeerId })
      await connectRepos(alice, bob)
      await connectRepos(alice, charlie)
      await connectRepos(bob, charlie)

      const aliceHandle = alice.create<TestDoc>({ foo: "bar" })
      const bobHandle = await bob.find<TestDoc>(aliceHandle.url)
      const charlieHandle = await charlie.find<TestDoc>(aliceHandle.url)
      await pause(50)

      const gotAtBob: unknown[] = []
      const gotAtCharlie: unknown[] = []
      bobHandle.on("ephemeral-message", p => gotAtBob.push(p.message))
      charlieHandle.on("ephemeral-message", p => gotAtCharlie.push(p.message))

      aliceHandle.broadcast({ hello: "everyone" })
      await pause(100)

      assert.deepStrictEqual(gotAtBob, [{ hello: "everyone" }])
      assert.deepStrictEqual(gotAtCharlie, [{ hello: "everyone" }])
    })
  })
})
