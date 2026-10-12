import { join } from "@std/path";
import { PeerStorage } from "./PeerStorage.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

function equal(actual: unknown, expected: unknown) {
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
    }
}

async function until(check: () => boolean | Promise<boolean>) {
    const deadline = Date.now() + 6000;
    while (!await check()) {
        if (Date.now() > deadline) throw new Error("Watcher did not converge");
        await new Promise(resolve => setTimeout(resolve, 20));
    }
}

async function fixture(work: (config: PeerStorageConf, peers: PeerStorage[]) => Promise<void>) {
    const root = await Deno.makeTempDir({ prefix: "storage-watch-" });
    const config: PeerStorageConf = { name: crypto.randomUUID(), type: "storage", baseDir: join(root, "vault"),
        useChokidar: false, scanOfflineChanges: false,
        writeProtocol: { lockPath: join(root, ".otask.lock"), versionsPath: join(root, "versions"), protectedPaths: ["tasks.md"] } };
    const peers: PeerStorage[] = [];
    await Deno.mkdir(config.baseDir);
    try { await work(config, peers); }
    finally {
        await Promise.all(peers.map(peer => peer.stop()));
        const prefix = `${config.name}-storage-`;
        const keys = Array.from({ length: localStorage.length }, (_, index) => localStorage.key(index));
        for (const key of keys) if (key?.startsWith(prefix)) localStorage.removeItem(key);
        await Deno.remove(root, { recursive: true });
    }
}

const data = (text: string, mtime = 1000): FileData => ({ data: [text], ctime: mtime, mtime, size: new TextEncoder().encode(text).length });
const text = (value: FileData | false) => value === false ? false : value.data instanceof Uint8Array ? new TextDecoder().decode(value.data) : value.data.join("");

for (const useChokidar of [true, false]) {
    Deno.test(`actual ${useChokidar ? "chokidar" : "Deno"} watcher retries after backend recovery without a new event`, () => fixture(async (config, peers) => {
        const path = join(config.baseDir, "tasks.md");
        await Deno.writeTextFile(path, "owner input\n");
        let available = false;
        let attempts = 0;
        const accepted: unknown[] = [];
        const peer = new PeerStorage({ ...config, useChokidar, scanOfflineChanges: true }, (_source, name, value) => {
            attempts++;
            if (!available) return Promise.reject(new Error("Backend unavailable"));
            accepted.push([name, text(value)]);
            return Promise.resolve();
        });
        peers.push(peer);
        await peer.start();
        await until(() => attempts > 0 && !!peer.health().detail?.includes("Backend unavailable"));
        equal(await peer.isChanged("tasks.md"), true);
        available = true;
        await until(() => accepted.length > 0 && !peer.health().detail?.includes("pending"));
        equal(accepted, [["tasks.md", "owner input\n"]]);
        equal(await peer.isChanged("tasks.md"), false);
    }));
}

Deno.test("inbound replacement waits for the outbound read and send of the same path", () => fixture(async (config, peers) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "owner input\n");
    let release!: () => void;
    const hold = new Promise<void>(resolve => release = resolve);
    let sending = false;
    const accepted: unknown[] = [];
    const peer = new PeerStorage(config, async (_source, _path, value) => {
        sending = true;
        accepted.push(text(value));
        await hold;
    });
    peers.push(peer);
    const send = peer.dispatch(path);
    try {
        await until(() => sending);
        const incoming = peer.put("tasks.md", data("remote input\n"));
        await new Promise(resolve => setTimeout(resolve, 40));
        equal(await Deno.readTextFile(path), "owner input\n");
        release();
        await send;
        await incoming;
        equal(accepted, ["owner input\n"]);
        equal(await Deno.readTextFile(path), "remote input\n");
    } finally { release(); await send; }
}));

Deno.test("accepted A then incoming B then local A is sent even with equal size and timestamp", () => fixture(async (config, peers) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "A\n");
    const accepted: unknown[] = [];
    const peer = new PeerStorage(config, (_source, _path, value) => {
        accepted.push(text(value));
        return Promise.resolve();
    });
    peers.push(peer);
    await peer.dispatch(path);
    await peer.put("tasks.md", data("B\n"));
    const stamp = (await Deno.stat(path)).mtime!;
    await Deno.writeTextFile(path, "A\n");
    await Deno.utime(path, stamp, stamp);
    await peer.dispatch(path);
    equal(accepted, ["A\n", "A\n"]);
}));

Deno.test("a refused observed unlink survives peer recreation without inferring other deletions", () => fixture(async (config, peers) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "owner input\n");
    const old = new PeerStorage(config, () => Promise.reject(new Error("Backend unavailable")));
    peers.push(old);
    await old.writeFileStat("tasks.md");
    await old.start();
    await Deno.remove(path);
    await until(() => !!old.getSetting("watch-pending-tasks.md"));
    await old.stop();
    for (const name of ["a.md", "b.md", "c.md"]) old.setSetting(`file-stat-${old.toLocalPath(name)}`, "1000-2");
    const accepted: unknown[] = [];
    const replacement = new PeerStorage(config, (_source, name, value) => {
        accepted.push([name, text(value)]);
        return Promise.resolve();
    });
    peers.push(replacement);
    await replacement.start();
    await until(() => accepted.length > 0 && !replacement.health().detail?.includes("pending"));
    equal(accepted, [["tasks.md", false]]);
    equal(replacement.getSetting("watch-pending-tasks.md"), null);
    equal(replacement.getSetting(`file-stat-${replacement.toLocalPath("tasks.md")}`), null);
}));

Deno.test("stop fences queued sends and callbacks from the old watcher", () => fixture(async (config, peers) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "owner input\n");
    let release!: () => void;
    const hold = new Promise<void>(resolve => release = resolve);
    const accepted: unknown[] = [];
    const peer = new PeerStorage(config, async (_source, _name, value) => { accepted.push(text(value)); await hold; });
    peers.push(peer);
    await peer.start();
    const send = peer.dispatch(path);
    try {
        await until(() => accepted.length === 1);
        peer.processFile({ kind: "modify", paths: [path] });
        await new Promise(resolve => setTimeout(resolve, 300));
        const stopping = peer.stop();
        release();
        await send;
        await stopping;
        await Deno.writeTextFile(path, "after stop\n");
        await new Promise(resolve => setTimeout(resolve, 350));
        equal(accepted, ["owner input\n"]);
    } finally { release(); await send; }
}));

Deno.test("missing root and non-file errors never send a deletion", () => fixture(async (config, peers) => {
    const sent: unknown[] = [];
    const peer = new PeerStorage(config, (_source, name, value) => {
        sent.push([name, text(value)]);
        return Promise.resolve();
    });
    peers.push(peer);
    await Deno.remove(config.baseDir);
    let rejected = false;
    try { await peer.dispatch(join(config.baseDir, "tasks.md")); } catch { rejected = true; }
    equal(rejected, true);
    equal(sent, []);
    equal(peer.getSetting("watch-pending-tasks.md"), null);
    await Deno.mkdir(config.baseDir);
    const parent = join(config.baseDir, "bad.md");
    await Deno.writeTextFile(parent, "file\n");
    rejected = false;
    try { await peer.dispatch(join(parent, "child.md")); } catch { rejected = true; }
    equal(rejected, true);
    equal(sent, []);
}));

Deno.test("a directory moved into the native watched vault sends its existing nested files", () => fixture(async (config, peers) => {
    const outside = join(config.baseDir, "..", "outside");
    await Deno.mkdir(join(outside, "nested"), { recursive: true });
    await Deno.writeTextFile(join(outside, "a.md"), "first\n");
    await Deno.writeTextFile(join(outside, "nested", "b.md"), "second\n");
    const accepted: unknown[][] = [];
    const peer = new PeerStorage(config, (_source, name, value) => {
        accepted.push([name, text(value)]);
        return Promise.resolve();
    });
    peers.push(peer);
    await peer.start();
    await Deno.rename(outside, join(config.baseDir, "moved"));
    await until(() => accepted.length === 2 && !peer.health().detail?.includes("pending"));
    equal(accepted.sort(), [["moved/a.md", "first\n"], ["moved/nested/b.md", "second\n"]]);
}));

Deno.test("a rescan event fixture sends current files and never infers missing files as deletions", () => fixture(async (config, peers) => {
    await Deno.writeTextFile(join(config.baseDir, "tasks.md"), "missed input\n");
    const accepted: unknown[] = [];
    const peer = new PeerStorage(config, (_source, name, value) => {
        accepted.push([name, text(value)]);
        return Promise.resolve();
    });
    peers.push(peer);
    peer.setSetting(`file-stat-${peer.toLocalPath("missing.md")}`, "1000-2");
    await peer.start();
    peer.processFile({ kind: "any", paths: [], flag: "rescan" });
    await until(() => accepted.length === 1 && !peer.health().detail?.includes("pending"));
    equal(accepted, [["tasks.md", "missed input\n"]]);
    equal(peer.getSetting("watch-pending-missing.md"), null);
}));
