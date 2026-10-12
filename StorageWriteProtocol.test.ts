import { join } from "@std/path";
import { PeerStorage } from "./PeerStorage.ts";
import { StorageWriteProtocol, replaceStorageFile } from "./StorageWriteProtocol.ts";
import type { Delivery, Origin, PeerStorageConf } from "./types.ts";

function equal(actual: unknown, expected: unknown) {
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
    }
}

async function fixture(operation: (root: string, config: PeerStorageConf) => Promise<void>) {
    const root = await Deno.makeTempDir({ prefix: "storage-protocol-" });
    const config: PeerStorageConf = { type: "storage", name: crypto.randomUUID(), baseDir: join(root, "vault"),
        writeProtocol: { lockPath: join(root, ".otask.lock"), versionsPath: join(root, ".otask-versions"),
            protectedPaths: ["tasks.md", "personal/daily/*.md"] } };
    await Deno.mkdir(config.baseDir);
    try {
        await operation(root, config);
    } finally {
        await Deno.remove(root, { recursive: true });
    }
}

const revision: Origin = { peer: "remote", rev: "2-incoming", winner: "2-incoming", revisions: { start: 2, ids: ["incoming", "base"] }, base: null };
const unguardedDelivery = (origin = revision): Delivery => ({ ...origin, apply: (write) => write() });

async function records(config: PeerStorageConf) {
    const result = [];
    for await (const entry of Deno.readDir(join(config.writeProtocol!.versionsPath, "records"))) {
        result.push(JSON.parse(await Deno.readTextFile(join(config.writeProtocol!.versionsPath, "records", entry.name))));
    }
    return result;
}

Deno.test("shared writer lock excludes a separate Linux flock process", () => fixture(async (_root, config) => {
    const protocol = new StorageWriteProtocol(config);
    const attempt = () => new Deno.Command("flock", {
        args: ["-n", config.writeProtocol!.lockPath, "true"], stdout: "piped", stderr: "piped",
    }).output();
    await protocol.locked(async () => equal((await attempt()).code, 1));
    equal((await attempt()).code, 0);
}));

Deno.test("received write preserves both complete versions and original source time", () => fixture(async (_root, config) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "owner checked B\n");
    const peer = new PeerStorage(config, async () => {});
    equal(await peer.put("tasks.md", { ctime: 0, mtime: 1791795000000, size: 16, data: ["offline V later\n"] }, unguardedDelivery()), true);
    equal(await Deno.readTextFile(path), "offline V later\n");
    const [record] = await records(config);
    equal(record.path, "tasks.md");
    equal(record.source_mtime, 1791795000000);
    equal(await Deno.readTextFile(join(config.writeProtocol!.versionsPath, "blobs", `${record.hash_before}.bin`)), "owner checked B\n");
    equal(await Deno.readTextFile(join(config.writeProtocol!.versionsPath, "blobs", `${record.hash_incoming}.bin`)), "offline V later\n");
    equal((await Deno.stat(path)).mtime?.getTime(), 1791795000000);
}));

Deno.test("concurrent peers preserve an unbroken chain of full inputs", () => fixture(async (_root, config) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "original\n");
    const left = new PeerStorage(config, async () => {});
    const right = new PeerStorage({ ...config, name: crypto.randomUUID() }, async () => {});
    equal(await Promise.all([
        left.put("tasks.md", { ctime: 0, mtime: 1791795000000, size: 9, data: ["left edit\n"] }, unguardedDelivery()),
        right.put("tasks.md", { ctime: 0, mtime: 1791795000001, size: 10, data: ["right edit\n"] }, unguardedDelivery()),
    ]), [true, true]);
    const saved = await records(config);
    equal(saved.length, 2);
    const bodies = new Set<string>();
    for (const record of saved) {
        for (const hash of [record.hash_before, record.hash_incoming]) {
            bodies.add(await Deno.readTextFile(join(config.writeProtocol!.versionsPath, "blobs", `${hash}.bin`)));
        }
    }
    equal([...bodies].sort(), ["left edit\n", "original\n", "right edit\n"]);
    const final = await Deno.readTextFile(path);
    equal(["left edit\n", "right edit\n"].includes(final), true);
    equal(saved.some((first) => saved.some((second) => first.id !== second.id && first.hash_incoming === second.hash_before)), true);
}));

Deno.test("remote deletion leaves recoverable owner bytes and a deletion input", () => fixture(async (_root, config) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "keep this owner task\n");
    const peer = new PeerStorage(config, async () => {});
    equal(await peer.delete("tasks.md", unguardedDelivery()), true);
    const [record] = await records(config);
    equal(record.hash_incoming, null);
    equal(await Deno.readTextFile(join(config.writeProtocol!.versionsPath, "blobs", `${record.hash_before}.bin`)), "keep this owner task\n");
    let missing = false;
    try { await Deno.stat(path); } catch (error) { missing = error instanceof Deno.errors.NotFound; }
    equal(missing, true);
}));

Deno.test("process crash after preservation leaves both inputs and permits retry", () => fixture(async (root, config) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "before crash\n");
    const module = new URL("./StorageWriteProtocol.ts", import.meta.url).href;
    const child = new Deno.Command(Deno.execPath(), { args: ["eval", `
        import {StorageWriteProtocol} from ${JSON.stringify(module)};
        const protocol = new StorageWriteProtocol(${JSON.stringify(config)});
        await protocol.locked(async () => {
            await protocol.preserve(${JSON.stringify(path)}, new TextEncoder().encode("after retry\\n"), 1791795000000);
            await Deno.writeTextFile(${JSON.stringify(join(root, "prepared"))}, "ready");
            await new Promise(() => setInterval(() => {}, 1000));
        });`], stdout: "piped", stderr: "piped" }).spawn();
    const deadline = Date.now() + 15000;
    try {
        while (true) {
            try { await Deno.stat(join(root, "prepared")); break; } catch (error) {
                if (!(error instanceof Deno.errors.NotFound)) throw error;
            }
            if (Date.now() > deadline) throw new Error("Crash worker did not preserve inputs");
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        child.kill("SIGKILL");
        await child.output();
        equal(await Deno.readTextFile(path), "before crash\n");
        equal((await records(config)).length, 1);
        const peer = new PeerStorage(config, async () => {});
        equal(await peer.put("tasks.md", { ctime: 0, mtime: 1791795000000, size: 12, data: ["after retry\n"] }, unguardedDelivery()), true);
        equal(await Deno.readTextFile(path), "after retry\n");
        equal((await records(config)).length, 2);
    } finally {
        try { child.kill("SIGKILL"); } catch { /* The crash worker may already have exited. */ }
    }
}));

Deno.test("atomic replacement preserves exact binary bytes", () => fixture(async (_root, config) => {
    const path = join(config.baseDir, "exact.ogg");
    const bytes = new Uint8Array([0, 255, 13, 10, 126, 127, 128]);
    await replaceStorageFile(path, bytes, 1791795000000);
    equal(Array.from(await Deno.readFile(path)), [0, 255, 13, 10, 126, 127, 128]);
}));

Deno.test("replayed bytes are a no-op but owner changes are checked again", () => fixture(async (_root, config) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "original\n");
    const peer = new PeerStorage(config, async () => {});
    const data = { ctime: 0, mtime: 1791795000000, size: 9, data: ["incoming\n"] };
    equal(await peer.put("tasks.md", data, unguardedDelivery()), true);
    equal(await peer.put("tasks.md", data, unguardedDelivery()), false);
    equal((await records(config)).length, 2);
    await Deno.writeTextFile(path, "owner changed again\n");
    equal(await peer.put("tasks.md", data, unguardedDelivery()), true);
    const saved = await records(config);
    equal(saved.length, 3);
    const previous = await Promise.all(saved.map((record) =>
        Deno.readTextFile(join(config.writeProtocol!.versionsPath, "blobs", `${record.hash_before}.bin`))));
    equal(previous.includes("owner changed again\n"), true);
}));

Deno.test("distinct remote revisions retain provenance even when bytes or absence are unchanged", () => fixture(async (_root, config) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "owner words\n");
    const peer = new PeerStorage(config, async () => {});
    const origin: Origin = { peer: "remote", rev: "2-edit", winner: "2-edit",
        revisions: { start: 2, ids: ["edit", "initial"] }, base: null };
    const incoming = { ctime: 0, mtime: 1791795000000, size: 12, data: ["owner words\n"] };
    equal(await peer.put("tasks.md", incoming, unguardedDelivery(origin)), false);
    const reverted: Origin = { ...origin, rev: "3-revert", winner: "3-revert",
        revisions: { start: 3, ids: ["revert", "edit", "initial"] } };
    equal(await peer.put("tasks.md", incoming, unguardedDelivery(reverted)), false);
    equal(await Deno.readTextFile(path), "owner words\n");
    const saved = await records(config);
    equal(saved.length, 2);
    equal(saved.map(record => record.origin.rev).sort(), ["2-edit", "3-revert"]);
    equal(saved.every(record => record.hash_before === record.hash_incoming), true);
    const deletion: Origin = { ...reverted, rev: "4-delete", winner: "4-delete",
        revisions: { start: 4, ids: ["delete", "revert", "edit", "initial"] } };
    equal(await peer.delete("tasks.md", unguardedDelivery(deletion)), true);
    const stillAbsent: Origin = { ...deletion, rev: "5-delete", winner: "5-delete", base: { rev: "4-delete", sha256: null },
        revisions: { start: 5, ids: ["delete", "delete", "revert", "edit", "initial"] } };
    equal(await peer.delete("tasks.md", unguardedDelivery(stillAbsent)), false);
    const deleted = (await records(config)).find(record => record.origin.rev === "5-delete");
    equal(deleted.origin, stillAbsent);
    equal([deleted.hash_before, deleted.hash_incoming], [null, null]);
}));

Deno.test("disk failure rejects the input and the same input can be retried", () => fixture(async (_root, config) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.mkdir(path);
    const peer = new PeerStorage(config, async () => {});
    const incoming = { ctime: 0, mtime: 1791795000000, size: 9, data: ["incoming\n"] };
    let rejected = false;
    try { await peer.put("tasks.md", incoming, unguardedDelivery()); } catch { rejected = true; }
    equal(rejected, true);
    equal((await Deno.stat(path)).isDirectory, true);
    await Deno.remove(path);
    equal(await peer.put("tasks.md", incoming, unguardedDelivery()), true);
    equal(await Deno.readTextFile(path), "incoming\n");
}));

Deno.test("failed outbound dispatch retains the change for a retry", () => fixture(async (_root, config) => {
    const path = join(config.baseDir, "tasks.md");
    await Deno.writeTextFile(path, "owner edit\n");
    let attempts = 0;
    const accepted: unknown[] = [];
    const peer = new PeerStorage(config, (_source, _path, value) => {
        if (++attempts === 1) return Promise.reject(new Error("Backend refused this version"));
        accepted.push(value === false ? false : value.data);
        return Promise.resolve();
    });
    let rejected = false;
    try { await peer.dispatch(path); } catch { rejected = true; }
    equal(rejected, true);
    equal(await peer.isChanged("tasks.md"), true);
    await Deno.writeTextFile(path, "new owner edit\n");
    await peer.dispatch(path);
    equal(attempts, 2);
    equal(accepted, [["new owner edit\n"]]);
    equal(await peer.isChanged("tasks.md"), false);
}));

Deno.test("a protected file takes only a delivery, and a delivery only a file the vault protects", () => fixture(async (_root, config) => {
    const board = join(config.baseDir, "tasks.md");
    const note = join(config.baseDir, "notes.md");
    await Deno.writeTextFile(board, "owner board\n");
    await Deno.writeTextFile(note, "owner note\n");
    const unprotected = { ...config, name: crypto.randomUUID(), writeProtocol: undefined };
    let applied = 0;
    const counted: Delivery = { ...revision, apply: (write) => { applied++; return write(); } };
    const data = { ctime: 0, mtime: 1791795000000, size: 9, data: ["incoming\n"] };
    const refusals = [
        () => new PeerStorage(config, async () => {}).put("tasks.md", data),
        () => new PeerStorage(config, async () => {}).delete("tasks.md"),
        () => new PeerStorage(config, async () => {}).put("notes.md", data, counted),
        () => new PeerStorage(unprotected, async () => {}).put("tasks.md", data, counted),
        () => new PeerStorage(unprotected, async () => {}).delete("tasks.md", counted),
    ];
    for (const refusal of refusals) {
        let rejected = false;
        try { await refusal(); } catch { rejected = true; }
        equal(rejected, true);
    }
    equal([applied, await Deno.readTextFile(board), await Deno.readTextFile(note)], [0, "owner board\n", "owner note\n"]);
    let recorded = false;
    try { recorded = (await records(config)).length > 0; } catch (error) { if (!(error instanceof Deno.errors.NotFound)) throw error; }
    equal(recorded, false);
    equal(await new PeerStorage(config, async () => {}).put("notes.md", data), true);
    equal(await Deno.readTextFile(note), "incoming\n");
}));
