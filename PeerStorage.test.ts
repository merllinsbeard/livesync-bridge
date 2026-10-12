import { PeerStorage } from "./PeerStorage.ts";
import type { PeerStorageConf } from "./types.ts";

function assert(condition: unknown, message: string) {
    if (!condition) throw new Error(message);
}

async function until(check: () => boolean) {
    const deadline = Date.now() + 2000;
    while (!check()) {
        if (Date.now() > deadline) throw new Error("Watcher lifecycle did not settle");
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}

async function fixture(useChokidar: boolean, work: (peer: PeerStorage) => Promise<void>) {
    const root = await Deno.makeTempDir({ prefix: "peer-storage-test-" });
    const config: PeerStorageConf = { type: "storage", name: crypto.randomUUID(), baseDir: root,
        scanOfflineChanges: false, useChokidar };
    const peer = new PeerStorage(config, async () => {});
    try { await work(peer); }
    finally { await peer.stop(); await Deno.remove(root, { recursive: true }); }
}

Deno.test("PeerStorage native Deno watcher clears health after its iterator ends", () => fixture(false, async peer => {
    await peer.start();
    assert(peer.health().ok, "Deno watcher should be healthy while running");
    peer.watcherDeno!.close();
    await until(() => !peer.health().ok);
}));

Deno.test("PeerStorage native Deno watcher survives overlapping stop and start", () => fixture(false, async peer => {
    await peer.start();
    const first = peer.watcherDeno;
    const stopping = peer.stop();
    const starting = peer.start();
    await Promise.all([stopping, starting]);
    assert(peer.watcherDeno !== first, "restart must create a fresh native watcher");
    assert(peer.health().ok, "old iterator cleanup must not clear the fresh watcher");
    await peer.stop();
    assert(!peer.health().ok, "stop must clear watcher health");
}));

Deno.test("PeerStorage native Chokidar error and stop clear health and close resources", () => fixture(true, async peer => {
    await peer.start();
    const failed = peer.watcher!;
    await new Promise<void>(resolve => failed.once("ready", resolve));
    failed.emit("error", new Error("in-process native watcher error event"));
    assert(!peer.health().ok, "native Chokidar error must clear health immediately");
    await failed.close();
    assert(Object.keys(failed.getWatched()).length === 0, "failed watcher must release watched directories");
    await peer.start();
    const current = peer.watcher!;
    const stopping = peer.stop();
    assert(!peer.health().ok, "stop must clear health immediately");
    await stopping;
    assert(Object.keys(current.getWatched()).length === 0, "stop must resolve after the real watcher closes");
    await peer.stop();
}));
