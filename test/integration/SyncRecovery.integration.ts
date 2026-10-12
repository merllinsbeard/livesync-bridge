import { dirname, fromFileUrl, join } from "@std/path";
import type { FilePathWithPrefix } from "@vrtmrz/livesync-commonlib/compat/common/types";
import { DEVICE_ID_PREFERRED, TweakValuesTemplate } from "@vrtmrz/livesync-commonlib/compat/common/types";
import { createTextBlob, extractObject } from "@vrtmrz/livesync-commonlib/compat/common/utils";
import { defaultLogger, setGlobalLogFunction } from "octagonal-wheels/common/logger";
import { Hub } from "../../Hub.ts";
import { CHANGES_SELECTOR, PeerCouchDB } from "../../PeerCouchDB.ts";
import type { Config, FileData, Origin, PeerCouchDBConf } from "../../types.ts";

const couchDbUrl = Deno.env.get("COUCHDB_URL") ?? "http://127.0.0.1:5989";
const couchDbUsername = Deno.env.get("COUCHDB_USERNAME") ?? "admin";
const couchDbPassword = Deno.env.get("COUCHDB_PASSWORD") ?? "testpassword";
const authHeader = `Basic ${btoa(`${couchDbUsername}:${couchDbPassword}`)}`;
const repositoryRoot = join(dirname(fromFileUrl(import.meta.url)), "..", "..");
const protectedPaths = ["tasks.md", "personal/daily/*.md"];

const serviceLogs: string[] = [];
setGlobalLogFunction((message) => {
    serviceLogs.push(String(message));
    if (serviceLogs.length > 400) serviceLogs.shift();
});

const unhandled: string[] = [];
globalThis.addEventListener("unhandledrejection", (event) => {
    event.preventDefault();
    unhandled.push(String(event.reason));
});

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(`${message}\n--- recent bridge log ---\n${serviceLogs.slice(-60).join("\n")}`);
}

function assertEquals<T>(actual: T, expected: T, message: string): void {
    assert(actual === expected, `${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
}

type CouchReply = { ok: boolean; status: number; body: any };

// Reads every reply body to the end, so no request of a test leaves a response open.
async function couch(path: string, init: RequestInit = {}): Promise<CouchReply> {
    const response = await fetch(`${couchDbUrl}${path}`, {
        ...init,
        headers: { authorization: authHeader, "content-type": "application/json", accept: "application/json" },
    });
    const text = await response.text();
    return { ok: response.ok, status: response.status, body: text ? JSON.parse(text) : undefined };
}

async function createVaultDatabase(initialised = true): Promise<string> {
    const database = `livesync_recovery_${crypto.randomUUID().replaceAll("-", "")}`;
    const created = await couch(`/${database}`, { method: "PUT" });
    assert(created.ok, `Could not create ${database}: HTTP ${created.status} ${JSON.stringify(created.body)}`);
    if (initialised) await writeMilestone(database);
    return database;
}

async function writeMilestone(database: string) {
    const milestone = await couch(`/${database}/_local/obsydian_livesync_milestone`, {
        method: "PUT",
        body: JSON.stringify({
            type: "milestoneinfo", created: Date.now(), accepted_nodes: [], node_info: {},
            locked: false, node_chunk_info: {}, tweak_values: {},
        }),
    });
    assert(milestone.ok, `Could not write the LiveSync milestone: HTTP ${milestone.status} ${JSON.stringify(milestone.body)}`);
}

async function updateSeq(database: string): Promise<string> {
    return String((await couch(`/${database}`)).body.update_seq);
}

// The changes after `since` that the bridge's feed would apply. update_seq cannot stand in for this: CouchDB's
// selector long poll reports no position for rejected rows that follow the last row it sends.
async function selectedChangesAfter(database: string, since: string): Promise<string[]> {
    const reply = await couch(`/${database}/_changes?filter=_selector&since=${encodeURIComponent(since)}`, {
        method: "POST",
        body: JSON.stringify({ selector: CHANGES_SELECTOR }),
    });
    assert(reply.ok, `Could not read the selected changes: HTTP ${reply.status} ${JSON.stringify(reply.body)}`);
    return (reply.body.results as { id: string; seq: string }[]).map((row) => `${row.id} at ${row.seq}`);
}

function seqNumber(seq: string | null): number {
    return Number(String(seq).split("-")[0]);
}

async function liveLeaves(database: string, id: string): Promise<{ rev: string; deleted: boolean }[]> {
    const body = (await couch(`/${database}/${encodeURIComponent(id)}?open_revs=all`)).body as { ok?: { _rev: string; _deleted?: boolean } }[];
    return body.filter((leaf) => leaf.ok).map((leaf) => ({ rev: leaf.ok!._rev, deleted: leaf.ok!._deleted === true }));
}

function couchConf(name: string, database: string, url = couchDbUrl, baseDir = ""): PeerCouchDBConf {
    return {
        type: "couchdb",
        name,
        group: "main",
        database,
        username: couchDbUsername,
        password: couchDbPassword,
        url,
        passphrase: "content-encryption-passphrase",
        obfuscatePassphrase: "path-obfuscation-passphrase",
        baseDir,
    };
}

function note(body: string, mtime = Date.now()): FileData {
    return { ctime: mtime, mtime, size: new TextEncoder().encode(body).length, data: [body] };
}

async function waitFor<T>(what: string | (() => string), probe: () => T | undefined | false | Promise<T | undefined | false>, timeoutMs = 45_000): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        const value = await probe();
        if (value !== undefined && value !== false) return value;
        if (Date.now() >= deadline) assert(false, `Timed out waiting for ${typeof what === "string" ? what : what()}`);
        await new Promise((resolve) => setTimeout(resolve, 200));
    }
}

type DeliveryRecord = Record<string, unknown> & { origin: Origin; hash_before: string | null; hash_incoming: string | null };

function lineage(origin: Origin): string[] {
    return origin.revisions.ids.map((id, index) => `${origin.revisions.start - index}-${id}`);
}

async function sha256Text(text: string): Promise<string> {
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)));
    return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function compact(database: string) {
    const started = await couch(`/${database}/_compact`, { method: "POST", body: "{}" });
    assert(started.ok, `Could not compact ${database}: HTTP ${started.status} ${JSON.stringify(started.body)}`);
    await waitFor("the compaction to finish", async () => (await couch(`/${database}`)).body.compact_running === false);
}

async function readText(path: string): Promise<string | undefined> {
    try {
        return await Deno.readTextFile(path);
    } catch (error) {
        if (error instanceof Deno.errors.NotFound) return undefined;
        throw error;
    }
}

async function startedPeer(conf: PeerCouchDBConf, dispatcher: PeerCouchDB["dispatchToHub"] = () => Promise.resolve()) {
    const peer = new PeerCouchDB(conf, dispatcher);
    await peer.start();
    return peer;
}

type BridgeOverrides = { url?: string; initialSince?: string; couchBaseDir?: string; useRemoteTweaks?: true };

class BridgeFixture {
    readonly vault: string;
    readonly versions: string;
    readonly name = `bridge-${crypto.randomUUID().slice(0, 8)}`;
    private hub?: Hub;

    constructor(readonly root: string, readonly database: string) {
        this.vault = join(root, "vault");
        this.versions = join(root, "signal", ".otask-versions");
    }

    config(overrides: BridgeOverrides = {}): Config {
        return {
            peers: [
                {
                    ...couchConf(`${this.name}-couch`, this.database, overrides.url, overrides.couchBaseDir),
                    writeProtocol: { protectedPaths, versionsPath: this.versions, initialSince: overrides.initialSince },
                    useRemoteTweaks: overrides.useRemoteTweaks,
                },
                {
                    type: "storage",
                    name: `${this.name}-storage`,
                    group: "main",
                    baseDir: this.vault,
                    scanOfflineChanges: true,
                    useChokidar: false,
                    writeProtocol: { lockPath: join(this.root, "signal", ".vault-writer.lock"), versionsPath: this.versions, protectedPaths },
                },
            ],
        };
    }

    get couchPeer(): PeerCouchDB {
        return this.hub!.peers[0] as PeerCouchDB;
    }

    async start(overrides: BridgeOverrides = {}) {
        await Deno.mkdir(this.vault, { recursive: true });
        this.hub = new Hub(this.config(overrides));
        this.hub.start();
        await waitFor("the bridge to report healthy sync", async () => (await this.hub!.healthProbe()).ok);
    }

    async stop() {
        if (!this.hub) return;
        const storage = this.hub.peers.find((peer) => peer.config.type === "storage")!;
        await storage.stop();
        await this.couchPeer.stop();
        this.hub = undefined;
    }

    checkpoint(): string | null {
        return new PeerCouchDB(this.config().peers[0] as PeerCouchDBConf, () => Promise.resolve()).getSetting("since");
    }

    forgetCheckpoint(legacyValue: string) {
        new PeerCouchDB(this.config().peers[0] as PeerCouchDBConf, () => Promise.resolve()).setSetting("since", legacyValue);
    }

    forgetBaseRevision(path: string) {
        const peer = new PeerCouchDB(this.config().peers[0] as PeerCouchDBConf, () => Promise.resolve());
        localStorage.removeItem(peer._getKey(`rev-${path}`));
    }

    async records(): Promise<Record<string, unknown>[]> {
        const result: Record<string, unknown>[] = [];
        try {
            for await (const entry of Deno.readDir(join(this.versions, "records"))) {
                result.push(JSON.parse(await Deno.readTextFile(join(this.versions, "records", entry.name))));
            }
        } catch (error) {
            if (!(error instanceof Deno.errors.NotFound)) throw error;
        }
        return result;
    }

    base(path: string): { rev: string; sha256: string | null } | null {
        return JSON.parse(this.couchPeer.getSetting(`rev-${path}`) ?? "null");
    }

    async storageHealth(): Promise<string> {
        return (await this.hub!.healthProbe()).peers[1].detail ?? "";
    }

    async couchHealth(): Promise<string> {
        return (await this.hub!.healthProbe()).peers[0].detail ?? "";
    }

    async couchBase(rev: string): Promise<Record<string, unknown>[]> {
        return (await this.records()).filter((record) => record.kind === "couch_base" && record.rev === rev);
    }

    async delivery(rev: string): Promise<DeliveryRecord> {
        return await waitFor(`the vault record of revision ${rev}`, async () =>
            (await this.records()).find((record) => (record.origin as Origin | undefined)?.rev === rev) as DeliveryRecord | undefined);
    }

    async blob(hash: unknown): Promise<string | undefined> {
        return typeof hash === "string" ? await readText(join(this.versions, "blobs", `${hash}.bin`)) : undefined;
    }
}

class BridgeProcess {
    private child?: Deno.ChildProcess;
    private piped: Promise<void>[] = [];
    private readonly configPath: string;
    private readonly healthPath: string;
    readonly output: string[] = [];

    constructor(private readonly fixture: BridgeFixture) {
        this.configPath = join(fixture.root, "config.json");
        this.healthPath = join(fixture.root, "health.json");
    }

    async start() {
        await Deno.mkdir(this.fixture.vault, { recursive: true });
        await Deno.writeTextFile(this.configPath, JSON.stringify(this.fixture.config()));
        await Deno.remove(this.healthPath).catch(() => {});
        this.child = new Deno.Command(Deno.execPath(), {
            args: ["run", "-A", "--location", `http://${this.fixture.name}.test`, "main.ts"],
            cwd: repositoryRoot,
            env: { LSB_CONFIG: this.configPath, LSB_HEALTH_FILE: this.healthPath },
            stdout: "piped",
            stderr: "piped",
        }).spawn();
        this.piped = [this.child.stdout, this.child.stderr].map((stream) =>
            stream.pipeThrough(new TextDecoderStream()).pipeTo(new WritableStream({
                write: (chunk) => {
                    this.output.push(chunk);
                    if (this.output.length > 200) this.output.shift();
                },
            }))
        );
        let health: string | undefined;
        try {
            await waitFor(() => `the bridge process to report healthy sync\n--- bridge process health ---\n${health ?? "(no health file)"}` +
                `\n--- bridge process output ---\n${this.output.join("").slice(-6000)}`, async () => {
                health = await readText(this.healthPath);
                return health !== undefined && JSON.parse(health).ok === true;
            });
        } catch (error) {
            await this.kill();
            throw error;
        }
    }

    async kill() {
        if (!this.child) return;
        try {
            this.child.kill("SIGKILL");
        } catch {
            // The bridge already exited; its status below still resolves.
        }
        await this.child.status;
        await Promise.all(this.piped);
        this.child = undefined;
    }
}

async function withVault(name: string, operation: (fixture: BridgeFixture, mac: PeerCouchDB) => Promise<void>, initialised = true) {
    const database = await createVaultDatabase(initialised);
    const root = await Deno.makeTempDir({ prefix: "lsb-recovery-" });
    const fixture = new BridgeFixture(root, database);
    const mac = await startedPeer(couchConf(`mac-${name}-${crypto.randomUUID().slice(0, 8)}`, database));
    try {
        await operation(fixture, mac);
    } finally {
        await fixture.stop();
        await mac.stop();
        await couch(`/${database}`, { method: "DELETE" });
        await Deno.remove(root, { recursive: true }).catch(() => {});
    }
}

function recoveryTest(name: string, fn: () => Promise<void>) {
    Deno.test({
        name,
        fn: async () => {
            unhandled.length = 0;
            await fn();
            assertEquals(unhandled.join("\n"), "", "no rejection may escape the bridge unhandled");
        },
    });
}

recoveryTest("CouchDB changes written while the bridge process was SIGKILLed reach the vault after restart", () =>
    withVault("downtime", async (fixture, mac) => {
        await mac.put("tasks.md", note("- [ ] baseline\n"));
        const bridge = new BridgeProcess(fixture);
        await bridge.start();
        await waitFor("the baseline board on disk", async () => await readText(join(fixture.vault, "tasks.md")) === "- [ ] baseline\n");
        await bridge.kill();

        await mac.put("tasks.md", note("- [ ] edited on the Mac while the bridge was down\n"));
        await mac.put("notes/offline.md", note("created while the bridge was down\n"));

        await bridge.start();
        try {
            await waitFor("the board edited during downtime", async () =>
                await readText(join(fixture.vault, "tasks.md")) === "- [ ] edited on the Mac while the bridge was down\n");
            await waitFor("the note created during downtime", async () =>
                await readText(join(fixture.vault, "notes", "offline.md")) === "created while the bridge was down\n");
        } catch (error) {
            throw new Error(`${error}\n--- bridge process output ---\n${bridge.output.join("").slice(-6000)}`);
        } finally {
            await bridge.kill();
        }
    }));

recoveryTest("a burst interrupted by SIGKILL converges on every final file after restart", () =>
    withVault("burst", async (fixture, mac) => {
        await mac.put("tasks.md", note("- [ ] baseline\n"));
        const bridge = new BridgeProcess(fixture);
        await bridge.start();
        const expected = new Map<string, string>();
        for (let index = 0; index < 40; index++) {
            const body = `burst note ${index}\n`;
            expected.set(`burst/${String(index).padStart(2, "0")}.md`, body);
            await mac.put(`burst/${String(index).padStart(2, "0")}.md`, note(body));
        }
        await waitFor("the first burst note on disk", async () => await readText(join(fixture.vault, "burst", "00.md")) !== undefined);
        await bridge.kill();

        await bridge.start();
        try {
            for (const [path, body] of expected) {
                await waitFor(`${path} after restart`, async () => await readText(join(fixture.vault, path)) === body);
            }
        } catch (error) {
            throw new Error(`${error}\n--- bridge process output ---\n${bridge.output.join("").slice(-6000)}`);
        } finally {
            await bridge.kill();
        }
    }));

class CouchProxy {
    private listener?: Deno.Listener;
    private accepting?: Promise<void>;
    private readonly connections = new Set<Deno.Conn>();
    private readonly pipes = new Set<Promise<unknown>>();
    private readonly upstream = new URL(couchDbUrl);
    port = 0;

    get url() {
        return `http://127.0.0.1:${this.port}`;
    }

    open() {
        this.listener = Deno.listen({ hostname: "127.0.0.1", port: this.port });
        this.port = (this.listener.addr as Deno.NetAddr).port;
        this.accepting = this.accept(this.listener);
    }

    private async accept(listener: Deno.Listener) {
        for await (const client of listener) {
            const server = await Deno.connect({ hostname: this.upstream.hostname, port: Number(this.upstream.port) });
            const close = () => {
                for (const conn of [client, server]) {
                    this.connections.delete(conn);
                    try {
                        conn.close();
                    } catch {
                        // The other direction or cut() already closed it.
                    }
                }
            };
            if (this.listener !== listener) {
                close();
                continue;
            }
            this.connections.add(client);
            this.connections.add(server);
            const pipe = Promise.all([
                client.readable.pipeTo(server.writable).catch(() => {}).finally(close),
                server.readable.pipeTo(client.writable).catch(() => {}).finally(close),
            ]);
            this.pipes.add(pipe);
            void pipe.finally(() => this.pipes.delete(pipe));
        }
    }

    async cut() {
        this.listener?.close();
        this.listener = undefined;
        for (const conn of this.connections) {
            try {
                conn.close();
            } catch {
                // A finished pipe already closed it.
            }
        }
        this.connections.clear();
        await Promise.all([this.accepting, ...this.pipes]);
    }
}

recoveryTest("changes written while the CouchDB connection is cut arrive after it returns", () =>
    withVault("reconnect", async (fixture, mac) => {
        const proxy = new CouchProxy();
        proxy.open();
        try {
            await mac.put("tasks.md", note("- [ ] baseline\n"));
            await fixture.start({ url: proxy.url });
            await waitFor("the baseline board on disk", async () => await readText(join(fixture.vault, "tasks.md")) === "- [ ] baseline\n");
            await fixture.stop();
            await fixture.start({ url: proxy.url });

            await proxy.cut();
            await mac.put("tasks.md", note("- [ ] edited while the connection was cut\n"));
            await mac.put("notes/during-outage.md", note("written during the outage\n"));
            await new Promise((resolve) => setTimeout(resolve, 3000));
            proxy.open();

            await waitFor("the board edited during the outage", async () =>
                await readText(join(fixture.vault, "tasks.md")) === "- [ ] edited while the connection was cut\n", 60_000);
            await waitFor("the note written during the outage", async () =>
                await readText(join(fixture.vault, "notes", "during-outage.md")) === "written during the outage\n", 60_000);
        } finally {
            await proxy.cut();
        }
    }));

recoveryTest("a failed vault write holds the checkpoint and is retried until it lands", () =>
    withVault("failed-write", async (fixture, mac) => {
        await fixture.start();
        await Deno.mkdir(join(fixture.vault, "blocked"), { recursive: true });
        await Deno.chmod(join(fixture.vault, "blocked"), 0o555);
        const beforeFailure = seqNumber(await updateSeq(fixture.database));

        await mac.put("blocked/held.md", note("must not be skipped\n"));
        await mac.put("open/after.md", note("applied after the failed write\n"));
        await waitFor("the later note on disk", async () => await readText(join(fixture.vault, "open", "after.md")) === "applied after the failed write\n");

        assertEquals(await readText(join(fixture.vault, "blocked", "held.md")), undefined, "the blocked write cannot have landed");
        const held = fixture.checkpoint();
        assert(seqNumber(held) <= beforeFailure,
            `the checkpoint must stay before the failed change: checkpoint=${held} beforeFailure=${beforeFailure}`);

        await Deno.chmod(join(fixture.vault, "blocked"), 0o755);
        await waitFor("the held note after the directory became writable", async () =>
            await readText(join(fixture.vault, "blocked", "held.md")) === "must not be skipped\n", 90_000);
        const head = seqNumber(await updateSeq(fixture.database));
        await waitFor("the checkpoint to pass both changes", () => seqNumber(fixture.checkpoint()) >= head);
    }));

recoveryTest("a vault edit based on a stale revision never overwrites a newer CouchDB revision", async () => {
    const database = await createVaultDatabase();
    const mac = await startedPeer(couchConf(`mac-cas-${crypto.randomUUID().slice(0, 8)}`, database));
    const delivered: string[] = [];
    let release = () => {};
    let gate: Promise<void> = Promise.resolve();
    const bridge = new PeerCouchDB({
        ...couchConf(`bridge-cas-${crypto.randomUUID().slice(0, 8)}`, database),
        writeProtocol: { protectedPaths },
    }, async (_source, path, data) => {
        await gate;
        if (data !== false) delivered.push(`${path}:${(data.data as string[]).join("")}`);
    });
    try {
        await mac.put("tasks.md", note("- [ ] base\n"));
        await bridge.start();
        await waitFor("the base revision delivered to the vault side", () => delivered.includes("tasks.md:- [ ] base\n"));

        gate = new Promise((resolve) => release = resolve);
        await mac.put("tasks.md", note("- [x] newer edit from the Mac\n"));
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        await waitFor("the Mac revision in CouchDB", async () => (await mac.get("tasks.md" as FilePathWithPrefix) as FileData).data.join("") === "- [x] newer edit from the Mac\n");

        let rejected: unknown;
        try {
            await bridge.put("tasks.md", note("- [ ] vault edit based on the base revision\n"));
        } catch (error) {
            rejected = error;
        }
        assert(rejected instanceof Error, "a write from a stale base revision must be refused, not acknowledged");
        const winner = await mac.get("tasks.md" as FilePathWithPrefix) as FileData;
        assertEquals(winner.data.join(""), "- [x] newer edit from the Mac\n", "the newer CouchDB revision must stay the winner");
        assertEquals((await liveLeaves(database, id)).length, 1, "no sibling revision may be created from the stale base");

        release();
        await waitFor("the newer revision delivered once the vault side is free",
            () => delivered.includes("tasks.md:- [x] newer edit from the Mac\n"));
    } finally {
        release();
        await bridge.stop();
        await mac.stop();
        await couch(`/${database}`, { method: "DELETE" });
    }
});

recoveryTest("both sides of a CouchDB conflict on the board stay recoverable for otask", () =>
    withVault("conflict", async (fixture, mac) => {
        await mac.put("tasks.md", note("- [ ] base\n"));
        await fixture.start();
        const board = join(fixture.vault, "tasks.md");
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        const [base] = await liveLeaves(fixture.database, id);

        await Deno.writeTextFile(board, "- [x] base\n- [ ] added on the server\n");
        await waitFor("the server edit in CouchDB", async () =>
            (await mac.get("tasks.md" as FilePathWithPrefix) as FileData).data.join("") === "- [x] base\n- [ ] added on the server\n");

        const offline = "- [ ] base\n- [ ] added offline on the Mac\n";
        const sibling = await mac.man.liveSyncLocalDB.putDBEntry({
            _id: id, path: "tasks.md" as FilePathWithPrefix, data: createTextBlob(offline), ctime: Date.now(), mtime: Date.now(),
            size: offline.length, type: "plain", datatype: "plain", eden: {}, children: [],
        }, false, base.rev);
        assert(sibling, "the offline Mac revision should be stored as a conflicting leaf");
        assertEquals((await liveLeaves(fixture.database, id)).length, 2, "CouchDB should now hold two live leaves");

        const conflict = await waitFor("the losing leaf preserved for otask", async () =>
            (await fixture.records()).find((record) => {
                const origin = record.origin as Origin | undefined;
                return origin !== undefined && origin.winner !== origin.rev;
            }) as DeliveryRecord | undefined);
        assert(lineage(conflict.origin).includes(base.rev), "the losing leaf must descend from the revision both edits started from");
        const losing = await fixture.blob(conflict.hash_incoming);
        const recoverable = new Set<string | undefined>([await readText(board)]);
        for (const record of await fixture.records()) {
            recoverable.add(await fixture.blob(record.hash_before));
            recoverable.add(await fixture.blob(record.hash_incoming));
        }
        assert(losing === offline || losing === "- [x] base\n- [ ] added on the server\n",
            `the preserved conflict bytes must be one of the two edits, got ${losing}`);
        assert(recoverable.has(offline), "the offline Mac edit must be on disk or in the version store");
        assert(recoverable.has("- [x] base\n- [ ] added on the server\n"), "the server edit must be on disk or in the version store");
        assertEquals((await liveLeaves(fixture.database, id)).length, 2, "the bridge must not delete or resolve either leaf");
    }));

recoveryTest("a first deploy seeded with the pre-stop update_seq replays the stop gap despite the legacy 'now' cursor", () =>
    withVault("seed", async (fixture, mac) => {
        await mac.put("tasks.md", note("- [ ] before the deploy\n"));
        await fixture.start();
        await waitFor("the pre-deploy board on disk", async () => await readText(join(fixture.vault, "tasks.md")) === "- [ ] before the deploy\n");
        const seed = await updateSeq(fixture.database);
        await fixture.stop();
        fixture.forgetCheckpoint("now");

        await mac.put("tasks.md", note("- [ ] edited during the deploy gap\n"));
        await fixture.start({ initialSince: seed });
        await waitFor("the board edited during the deploy gap", async () =>
            await readText(join(fixture.vault, "tasks.md")) === "- [ ] edited during the deploy gap\n");
        assert(seqNumber(fixture.checkpoint()) >= seqNumber(seed), `the cursor must be concrete after start, got ${fixture.checkpoint()}`);
    }));

async function stopAsLegacyBridge(fixture: BridgeFixture) {
    await fixture.stop();
    fixture.forgetCheckpoint("now");
    fixture.forgetBaseRevision("tasks.md");
}

async function winner(database: string, id: string): Promise<{ _rev: string; deleted?: boolean }> {
    return (await couch(`/${database}/${encodeURIComponent(id)}`)).body;
}

recoveryTest("a vault edit with no recorded base never overwrites a CouchDB revision the vault did not receive", () =>
    withVault("no-base-edit", async (fixture, mac) => {
        await mac.put("tasks.md", note("- [ ] base\n"));
        await fixture.start();
        const board = join(fixture.vault, "tasks.md");
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        await stopAsLegacyBridge(fixture);

        const macEdit = "- [x] base\n- [ ] added on the Mac while the bridge was down\n";
        const serverEdit = "- [ ] base\n- [ ] added on the server while the bridge was down\n";
        await mac.put("tasks.md", note(macEdit));
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        const macRevision = (await winner(fixture.database, id))._rev;
        await Deno.writeTextFile(board, serverEdit);

        await fixture.start();
        await waitFor("the Mac revision delivered to the vault", async () => await readText(board) === macEdit);
        const leaves = await liveLeaves(fixture.database, id);
        assertEquals(leaves.length, 1, "no sibling revision may be created");
        assertEquals(leaves[0].rev, macRevision, "the Mac revision must stay the only leaf");
        assert((await winner(fixture.database, id)).deleted !== true, "the Mac revision must not be marked deleted");
        const kept = new Set<string | undefined>();
        for (const record of await fixture.records()) {
            kept.add(await fixture.blob(record.hash_before));
            kept.add(await fixture.blob(record.hash_incoming));
        }
        assert(kept.has(serverEdit), "the server edit must stay in the version store for otask");
    }));

recoveryTest("a vault deletion with no recorded base never deletes a CouchDB revision the vault did not receive", () =>
    withVault("no-base-delete", async (fixture, mac) => {
        await mac.put("tasks.md", note("- [ ] base\n"));
        await fixture.start();
        const board = join(fixture.vault, "tasks.md");
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        await stopAsLegacyBridge(fixture);

        const macEdit = "- [x] base\n- [ ] added on the Mac while the bridge was down\n";
        await mac.put("tasks.md", note(macEdit));
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        const macRevision = (await winner(fixture.database, id))._rev;

        await fixture.start();
        await Deno.mkdir(join(fixture.vault, "notes"), { recursive: true });
        await Deno.writeTextFile(join(fixture.vault, "notes", "watcher-ready.md"), "ready\n");
        await waitFor("the vault watcher to pass a change to CouchDB", async () =>
            (await mac.get("notes/watcher-ready.md" as FilePathWithPrefix) as false | FileData || undefined)?.data.join("") === "ready\n");
        await Deno.remove(board);
        await waitFor("the Mac revision restored in the vault", async () => await readText(board) === macEdit);
        const current = await winner(fixture.database, id);
        assertEquals(current._rev, macRevision, "the Mac revision must stay the winner");
        assert(current.deleted !== true, "the Mac revision must not be marked deleted");
        assertEquals((await liveLeaves(fixture.database, id)).length, 1, "no sibling revision may be created");
    }));

recoveryTest("a bridge started on an empty remote follows it once a LiveSync client initialises it", () =>
    withVault("empty-remote", async (fixture, mac) => {
        await fixture.start();
        await writeMilestone(fixture.database);
        await mac.put("tasks.md", note("- [ ] first board on the initialised remote\n"));
        await waitFor("the board from the initialised remote", async () =>
            await readText(join(fixture.vault, "tasks.md")) === "- [ ] first board on the initialised remote\n");
    }, false));

recoveryTest("a remote rebuilt under a running bridge is followed from its start and keeps the vault edit made meanwhile", () =>
    withVault("rebuilt-remote", async (fixture, mac) => {
        await mac.put("tasks.md", note("- [ ] before the rebuild\n"));
        await fixture.start();
        const board = join(fixture.vault, "tasks.md");
        await waitFor("the board before the rebuild", async () => await readText(board) === "- [ ] before the rebuild\n");

        const removed = await couch(`/${fixture.database}`, { method: "DELETE" });
        assert(removed.ok, `Could not delete ${fixture.database}: HTTP ${removed.status} ${JSON.stringify(removed.body)}`);
        const recreated = await couch(`/${fixture.database}`, { method: "PUT" });
        assert(recreated.ok || recreated.status === 412, `Could not recreate ${fixture.database}: HTTP ${recreated.status} ${JSON.stringify(recreated.body)}`);
        await writeMilestone(fixture.database);
        const vaultEdit = "- [ ] before the rebuild\n- [ ] added on the server during the rebuild\n";
        await Deno.writeTextFile(board, vaultEdit);

        const uploader = await startedPeer(couchConf(`rebuild-${crypto.randomUUID().slice(0, 8)}`, fixture.database));
        try {
            const uploaded = "- [x] before the rebuild\n- [ ] uploaded by the rebuild\n";
            await uploader.put("tasks.md", note(uploaded));
            await uploader.put("notes/after-rebuild.md", note("uploaded by the rebuild\n"));
            await waitFor("the rebuilt board in the vault", async () => await readText(board) === uploaded, 90_000);
            await waitFor("a note that exists only in the rebuilt remote", async () =>
                await readText(join(fixture.vault, "notes", "after-rebuild.md")) === "uploaded by the rebuild\n");

            const id = await uploader.man.path2id("tasks.md" as FilePathWithPrefix);
            assertEquals((await liveLeaves(fixture.database, id)).length, 1, "the rebuilt board must stay a single leaf");
            assertEquals((await uploader.get("tasks.md" as FilePathWithPrefix) as FileData).data.join(""), uploaded,
                "the board uploaded by the rebuild must stay the winner");
            const kept = new Set<string | undefined>();
            for (const record of await fixture.records()) {
                kept.add(await fixture.blob(record.hash_before));
                kept.add(await fixture.blob(record.hash_incoming));
            }
            assert(kept.has(vaultEdit), "the server edit made during the rebuild must stay in the version store");
            const rebuilt = await sha256Text(uploaded);
            const delivered = (await fixture.records()).find((record) => record.origin && record.hash_incoming === rebuilt) as DeliveryRecord | undefined;
            assert(delivered?.origin.base, "the rebuilt board must arrive with the base from the old remote");
            assert(!lineage(delivered.origin).includes(delivered.origin.base.rev),
                "a rebuilt remote with new history must not claim the old base as an ancestor");
        } finally {
            await uploader.stop();
        }
    }));

recoveryTest("with remote tweaks on, as in production, the bridge re-initialises on the Mac's tweak values, delivers the board and stops with no feed left", () =>
    withVault("remote-tweaks", async (fixture, mac) => {
        const tweaks = extractObject(TweakValuesTemplate, mac.man.settings);
        const milestonePath = `/${fixture.database}/_local/obsydian_livesync_milestone`;
        const milestone = (await couch(milestonePath)).body;
        const recorded = await couch(milestonePath, {
            method: "PUT",
            body: JSON.stringify({ ...milestone, accepted_nodes: [mac.config.name], tweak_values: { [mac.config.name]: tweaks, [DEVICE_ID_PREFERRED]: tweaks } }),
        });
        assert(recorded.ok, `Could not record the tweak values: HTTP ${recorded.status} ${JSON.stringify(recorded.body)}`);
        const board = "- [ ] delivered after the remote tweaks were applied\n";
        await mac.put("tasks.md", note(board));
        const unset = fixture.config().peers[0] as PeerCouchDBConf;
        assert(unset.hashAlg === undefined && unset.customChunkSize === undefined, "the bridge's own config leaves the tweaked values unset");

        await fixture.start({ useRemoteTweaks: true });
        assertEquals(fixture.couchPeer.config.hashAlg, tweaks.hashAlg, "the bridge adopts the Mac's hash algorithm");
        assertEquals(fixture.couchPeer.config.customChunkSize, tweaks.customChunkSize, "the bridge adopts the Mac's chunk size");
        assert(serviceLogs.some((line) => line.startsWith(`[${fixture.name}-couch] Remote tweaks changed`)),
            "the bridge reports the tweaks it applied after initialising again");

        await waitFor("the board on disk", async () => await readText(join(fixture.vault, "tasks.md")) === board);
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        assertEquals(JSON.stringify(fixture.base("tasks.md")), JSON.stringify({ rev: (await winner(fixture.database, id))._rev, sha256: await sha256Text(board) }),
            "the delivered board becomes the verified base");
    }));

recoveryTest("vault and Mac edits, a revert included, reach the vault as one proven line of revisions", () =>
    withVault("lineage", async (fixture, mac) => {
        const board = join(fixture.vault, "tasks.md");
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        assert(id.startsWith("f:"), `the fixture must obfuscate paths as production does, got ${id}`);
        await mac.put("tasks.md", note("- [ ] call the bank\n"));
        await fixture.start();
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] call the bank\n");

        const vaultEdit = "- [ ] call the bank\n- [ ] renew the passport\n";
        await Deno.writeTextFile(board, vaultEdit);
        await waitFor("the vault edit in CouchDB", async () => (await mac.get("tasks.md" as FilePathWithPrefix) as FileData).data.join("") === vaultEdit);
        const stored = (await winner(fixture.database, id))._rev;
        const [storedBase] = await waitFor("the couch_base record of the stored vault edit", async () => {
            const found = await fixture.couchBase(stored);
            return found.length > 0 && found;
        });
        assertEquals(storedBase.hash_incoming, await sha256Text(vaultEdit), "couch_base ties the stored revision to the exact vault bytes");
        assertEquals(storedBase.peer, `${fixture.name}-couch`, "couch_base names the CouchDB peer");

        const checked = "- [x] call the bank\n- [ ] renew the passport\n";
        await mac.put("tasks.md", note(checked));
        const checkedRev = (await winner(fixture.database, id))._rev;
        const check = await fixture.delivery(checkedRev);
        await waitFor("the checked board on disk", async () => await readText(board) === checked);

        await mac.put("tasks.md", note(vaultEdit));
        const revertRev = (await winner(fixture.database, id))._rev;
        const revert = await fixture.delivery(revertRev);
        await waitFor("the reverted board on disk", async () => await readText(board) === vaultEdit);

        assertEquals(check.origin.peer, `${fixture.name}-couch`, "the origin names the CouchDB peer");
        assertEquals(check.origin.winner, checkedRev, "a winner delivery names itself as the winner");
        assertEquals(check.origin.base?.rev, stored, "the vault edit stored in CouchDB is the base of the next delivery");
        assertEquals(check.origin.base?.sha256, await sha256Text(vaultEdit), "the base hash is the SHA-256 of the stored vault bytes");
        assertEquals(check.hash_before, check.origin.base?.sha256, "the vault held exactly the base before the checked board");
        assertEquals(lineage(check.origin)[1], stored, "the checked board is a child of the stored vault edit");

        assertEquals(revert.origin.base?.rev, checkedRev, "the delivered checked board is the base of the revert");
        assertEquals(revert.origin.base?.sha256, check.hash_incoming, "the base hash equals the hash the vault recorded for the checked board");
        assertEquals(revert.hash_before, revert.origin.base?.sha256, "the vault held exactly the base before the revert");
        assertEquals(revert.hash_incoming, await sha256Text(vaultEdit), "the revert brings back the earlier bytes");
        const line = lineage(revert.origin);
        assert(line.includes(checkedRev) && line.includes(stored),
            `the revert must descend from the checked board and the stored vault edit: ${line.join(" ")}`);
        assertEquals((await fixture.couchBase(stored)).length, 1, "the stored vault edit is recorded once");

        await Deno.remove(board);
        const deletion = await waitFor("the vault deletion in CouchDB", async () => {
            const current = await winner(fixture.database, id) as { _rev: string; deleted?: boolean };
            return current.deleted === true && current;
        });
        const [deletionBase] = await waitFor("the couch_base record of the vault deletion", async () => {
            const found = await fixture.couchBase(deletion._rev);
            return found.length > 0 && found;
        });
        assertEquals(deletionBase.hash_incoming, null, "a confirmed deletion is recorded with no bytes");
    }));

recoveryTest("a vault write whose couch_base record fails stays pending and is confirmed from a fresh read without a second revision", () =>
    withVault("base-record-fault", async (fixture, mac) => {
        const board = join(fixture.vault, "tasks.md");
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        await mac.put("tasks.md", note("- [ ] base\n"));
        await fixture.start();
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        const records = join(fixture.versions, "records");
        await waitFor("the version store", async () => (await Deno.stat(records).catch(() => undefined))?.isDirectory);

        const edit = "- [x] base\n- [ ] edited in the vault\n";
        let stored = "";
        await Deno.chmod(records, 0o555);
        try {
            await Deno.writeTextFile(board, edit);
            await waitFor("the vault edit in CouchDB", async () => (await mac.get("tasks.md" as FilePathWithPrefix) as FileData).data.join("") === edit);
            stored = (await winner(fixture.database, id))._rev;
            await waitFor("the unrecorded write reported as pending vault work", async () => {
                const detail = await fixture.storageHealth();
                return detail.includes("pending") && detail.includes("tasks.md");
            });
            assert(fixture.base("tasks.md")?.rev !== stored, "the base must not move to the stored revision before its record exists");
        } finally {
            await Deno.chmod(records, 0o755);
        }

        await waitFor("the vault write acknowledged after the version store recovered", async () => !(await fixture.storageHealth()).includes("pending"), 90_000);
        const tied = (await fixture.records()).filter((record) => record.rev === stored || (record.origin as Origin | undefined)?.rev === stored);
        assertEquals(JSON.stringify(tied.map((record) => record.kind)), JSON.stringify(["couch_base"]),
            "the stored revision must be recorded once, as the vault's own write and not as an incoming echo");
        assertEquals(tied[0].hash_incoming, await sha256Text(edit), "the record holds the exact vault bytes");
        assertEquals(JSON.stringify(fixture.base("tasks.md")), JSON.stringify({ rev: stored, sha256: await sha256Text(edit) }),
            "once recorded, the stored revision is the base");
        assertEquals((await winner(fixture.database, id))._rev, stored, "the retry confirms the stored revision instead of writing a new one");
        assertEquals((await liveLeaves(fixture.database, id)).length, 1, "no sibling revision may appear");

        await mac.put("tasks.md", note("- [x] base\n- [x] edited in the vault\n"));
        const next = await fixture.delivery((await winner(fixture.database, id))._rev);
        assertEquals(next.origin.base?.rev, stored, "the confirmed revision is the base of the next delivery");
        assertEquals(next.origin.base?.sha256, await sha256Text(edit), "with the hash of the vault bytes");
    }));

recoveryTest("a vault edit saved while the record of the previous vault write fails survives that write's echo and becomes the base after it", () =>
    withVault("base-record-fault-edit", async (fixture, mac) => {
        const board = join(fixture.vault, "tasks.md");
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        await mac.put("tasks.md", note("- [ ] base\n"));
        await fixture.start();
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        const records = join(fixture.versions, "records");
        await waitFor("the version store", async () => (await Deno.stat(records).catch(() => undefined))?.isDirectory);

        const first = "- [x] base\n- [ ] first vault edit\n";
        const second = "- [x] base\n- [x] first vault edit\n- [ ] second vault edit\n";
        let unrecorded = "";
        let sampling = true;
        let reverted = false;
        let sampler = Promise.resolve();
        await Deno.chmod(records, 0o555);
        try {
            await Deno.writeTextFile(board, first);
            await waitFor("the first vault edit in CouchDB", async () => (await mac.get("tasks.md" as FilePathWithPrefix) as FileData).data.join("") === first);
            unrecorded = (await winner(fixture.database, id))._rev;
            await waitFor("the echo of the unrecorded write held in the changes feed", async () => (await fixture.couchHealth()).includes(`retrying ${id}`));
            assert(fixture.base("tasks.md")?.rev !== unrecorded, "the base must not move to the unrecorded revision");

            await Deno.writeTextFile(board, second);
            sampler = (async () => {
                while (sampling) {
                    if (await readText(board) !== second) reverted = true;
                    await new Promise((resolve) => setTimeout(resolve, 50));
                }
            })();
            const secondWriteObservationMs = 2_000;
            await new Promise((resolve) => setTimeout(resolve, secondWriteObservationMs));
            assertEquals((await winner(fixture.database, id))._rev, unrecorded, "no revision may be built on a write whose record is missing");
        } finally {
            await Deno.chmod(records, 0o755);
        }

        try {
            let state = "";
            await waitFor(() => `the vault edits acknowledged and the feed caught up (${state})`, async () => {
                const storage = await fixture.storageHealth();
                const couchDetail = await fixture.couchHealth();
                const since = fixture.couchPeer.getSetting("since");
                const head = await updateSeq(fixture.database);
                const unapplied = await selectedChangesAfter(fixture.database, since!);
                state = `storage: ${storage}; couch: ${couchDetail}; checkpoint: ${since}; update_seq: ${head}; selected changes after the checkpoint: ${JSON.stringify(unapplied)}`;
                return !storage.includes("pending") && couchDetail === "watching" && unapplied.length === 0;
            }, 90_000);
        } finally {
            sampling = false;
            await sampler;
        }
        assert(!reverted, "the second vault edit must stay on disk through the echo of the first");
        const confirmed = (await couch(`/${fixture.database}/${encodeURIComponent(id)}?revs=true`)).body as { _rev: string; _revisions: Origin["revisions"] };
        assertEquals((await mac.get("tasks.md" as FilePathWithPrefix) as FileData).data.join(""), second, "CouchDB holds the second vault edit");
        assert(lineage({ revisions: confirmed._revisions } as Origin).includes(unrecorded), "the second edit is stored on top of the first");
        for (const [rev, text] of [[unrecorded, first], [confirmed._rev, second]]) {
            const tied = (await fixture.records()).filter((record) => record.rev === rev || (record.origin as Origin | undefined)?.rev === rev);
            assertEquals(JSON.stringify(tied.map((record) => record.kind)), JSON.stringify(["couch_base"]),
                `revision ${rev} must be recorded once, as the vault's own write and not as an incoming echo`);
            assertEquals(tied[0].hash_incoming, await sha256Text(text), `the record of ${rev} holds its exact vault bytes`);
        }
        assertEquals(JSON.stringify(fixture.base("tasks.md")), JSON.stringify({ rev: confirmed._rev, sha256: await sha256Text(second) }),
            "the base ends at the second edit and never returns to the first");
        assertEquals((await liveLeaves(fixture.database, id)).length, 1, "no sibling revision may appear");
    }));

recoveryTest("a vault edit made over the base while the bridge was down is kept and named as a divergence from the base", () =>
    withVault("local-divergence", async (fixture, mac) => {
        const board = join(fixture.vault, "tasks.md");
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        await mac.put("tasks.md", note("- [ ] base\n"));
        await fixture.start();
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        const base = (await winner(fixture.database, id))._rev;
        await fixture.stop();

        const local = "- [ ] base\n- [ ] added in the vault while the bridge was down\n";
        await Deno.writeTextFile(board, local);
        const macEdit = "- [x] base\n";
        await mac.put("tasks.md", note(macEdit));
        const macRev = (await winner(fixture.database, id))._rev;

        await fixture.start();
        const record = await fixture.delivery(macRev);
        await waitFor("the Mac edit on disk", async () => await readText(board) === macEdit);
        assertEquals(record.origin.base?.rev, base, "the delivery names the base the vault last held");
        assert(lineage(record.origin).includes(base), "the Mac edit descends from that base");
        assertEquals(record.hash_before, await sha256Text(local), "the record names the vault edit as the bytes before");
        assert(record.hash_before !== record.origin.base?.sha256, "the vault bytes differ from the base, so the edit is a divergence");
        assertEquals(await fixture.blob(record.hash_before), local, "the vault edit stays in the version store");
    }));

recoveryTest("a LiveSync soft deletion reaches the vault and a recreation names the deletion as its base", () =>
    withVault("soft-delete", async (fixture, mac) => {
        const board = join(fixture.vault, "tasks.md");
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        await mac.put("tasks.md", note("- [ ] base\n"));
        await fixture.start();
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        const base = (await winner(fixture.database, id))._rev;

        assert(await mac.delete("tasks.md"), "the Mac should delete the board");
        const deletion = await winner(fixture.database, id) as { _rev: string; deleted?: boolean; _deleted?: boolean };
        assert(deletion.deleted === true && deletion._deleted !== true, `LiveSync should keep the document with deleted: true, got ${JSON.stringify(deletion)}`);
        const removed = await fixture.delivery(deletion._rev);
        await waitFor("the board removed from the vault", async () => await readText(board) === undefined);
        assertEquals(removed.hash_incoming, null, "a deletion brings no bytes");
        assertEquals(removed.origin.base?.rev, base, "the deletion names the base the vault held");
        assertEquals(lineage(removed.origin)[1], base, "the deleted revision is the parent of the deletion");

        await mac.put("tasks.md", note("- [ ] recreated on the Mac\n"));
        const recreatedRev = (await winner(fixture.database, id))._rev;
        const recreated = await fixture.delivery(recreatedRev);
        await waitFor("the recreated board on disk", async () => await readText(board) === "- [ ] recreated on the Mac\n");
        assertEquals(JSON.stringify(recreated.origin.base), JSON.stringify({ rev: deletion._rev, sha256: null }),
            "a delivered deletion is the base, with no bytes");
        assert(lineage(recreated.origin).includes(deletion._rev), "the recreation descends from the deletion");
    }));

recoveryTest("a LiveSync deletion that removes the metadata reaches the vault, and the vault can recreate the file", () =>
    withVault("metadata-delete", async (fixture, mac) => {
        const board = join(fixture.vault, "tasks.md");
        const id = await mac.man.path2id("tasks.md" as FilePathWithPrefix);
        await mac.put("tasks.md", note("- [ ] base\n"));
        await fixture.start();
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        const base = (await winner(fixture.database, id))._rev;

        // The LiveSync option "delete metadata of deleted files": the tombstone keeps the body under _deleted.
        mac.man.services.setting.settings.deleteMetadataOfDeletedFiles = true;
        assert(await mac.delete("tasks.md"), "the Mac should delete the board");
        const [tombstone] = await liveLeaves(fixture.database, id);
        assert(tombstone?.deleted, `the document should now be a tombstone, got ${JSON.stringify(tombstone)}`);
        const removed = await fixture.delivery(tombstone.rev);
        await waitFor("the board removed from the vault", async () => await readText(board) === undefined);
        assertEquals(lineage(removed.origin)[1], base, "the deleted revision is the parent of the tombstone");

        const recreated = "- [ ] recreated in the vault\n";
        await Deno.writeTextFile(board, recreated);
        await waitFor("the vault recreation in CouchDB", async () =>
            (await mac.get("tasks.md" as FilePathWithPrefix) as false | FileData || undefined)?.data.join("") === recreated);
        const leaves = await liveLeaves(fixture.database, id);
        assertEquals(leaves.length, 1, "the recreation must extend the tombstone, not start a sibling");
        assert(!leaves[0].deleted, "the recreated board must be live");
    }));

recoveryTest("a bodiless deletion made while the bridge was down reaches the vault after compaction dropped the deleted revision", () =>
    withVault("bodiless-delete", async (fixture, mac) => {
        const shared = { couchBaseDir: "shared/" };
        const board = join(fixture.vault, "tasks.md");
        const id = await mac.man.path2id("shared/tasks.md" as FilePathWithPrefix);
        await mac.put("shared/tasks.md", note("- [ ] base\n"));
        await fixture.start(shared);
        await waitFor("the base board on disk", async () => await readText(board) === "- [ ] base\n");
        const base = (await winner(fixture.database, id))._rev;
        await fixture.stop();

        const deleted = await couch(`/${fixture.database}/${encodeURIComponent(id)}?rev=${base}`, { method: "DELETE" });
        assert(deleted.ok, `Could not delete the board: HTTP ${deleted.status} ${JSON.stringify(deleted.body)}`);
        const tombstone = deleted.body.rev as string;
        await compact(fixture.database);
        const parent = await couch(`/${fixture.database}/${encodeURIComponent(id)}?rev=${base}`);
        assert(!parent.ok, `compaction should have dropped the deleted revision, got HTTP ${parent.status} ${JSON.stringify(parent.body)}`);

        await fixture.start(shared);
        const removed = await fixture.delivery(tombstone);
        await waitFor("the board removed from the vault", async () => await readText(board) === undefined);
        assertEquals(removed.origin.base?.rev, base, "the deletion names the base the vault held");
        assertEquals(lineage(removed.origin)[1], base, "the deleted revision is the parent of the tombstone");
    }));

recoveryTest("teardown", () => {
    setGlobalLogFunction(defaultLogger);
    return Promise.resolve();
});
