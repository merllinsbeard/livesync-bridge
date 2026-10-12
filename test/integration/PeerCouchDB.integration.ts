import type { FilePathWithPrefix } from "@vrtmrz/livesync-commonlib/compat/common/types";
import { defaultLogger, setGlobalLogFunction } from "octagonal-wheels/common/logger";
import { PeerCouchDB } from "../../PeerCouchDB.ts";
import type { FileData, PeerCouchDBConf } from "../../types.ts";

const couchDbUrl = Deno.env.get("COUCHDB_URL") ?? "http://127.0.0.1:5989";
const couchDbUsername = Deno.env.get("COUCHDB_USERNAME") ?? "admin";
const couchDbPassword = Deno.env.get("COUCHDB_PASSWORD") ?? "testpassword";
const authHeader = `Basic ${btoa(`${couchDbUsername}:${couchDbPassword}`)}`;

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function assertEquals<T>(actual: T, expected: T, message: string): void {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}

async function waitForCouchDb(): Promise<void> {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
        try {
            const response = await fetch(`${couchDbUrl}/_up`, { headers: { authorization: authHeader } });
            await response.body?.cancel();
            if (response.ok) return;
        } catch {
            // CouchDB is still starting.
        }
        await new Promise((resolve) => setTimeout(resolve, 500));
    }
    throw new Error(`CouchDB did not become ready at ${couchDbUrl} within 60 seconds`);
}

async function requestDatabase(database: string, method: "PUT" | "DELETE"): Promise<Response> {
    return await fetch(`${couchDbUrl}/${database}`, {
        method,
        headers: { authorization: authHeader },
    });
}

function makeConfig(name: string, database: string): PeerCouchDBConf {
    return {
        type: "couchdb",
        name,
        database,
        username: couchDbUsername,
        password: couchDbPassword,
        url: couchDbUrl,
        passphrase: "content-encryption-passphrase",
        obfuscatePassphrase: "path-obfuscation-passphrase",
        baseDir: "",
    };
}

Deno.test("PeerCouchDB exchanges files with distinct content and path passphrases", async () => {
    await waitForCouchDb();

    const suffix = crypto.randomUUID().replaceAll("-", "");
    const database = `livesync_bridge_${suffix}`;
    const created = await requestDatabase(database, "PUT");
    assert(created.ok, `Could not create integration database: HTTP ${created.status} ${await created.text()}`);

    const peerA = new PeerCouchDB(makeConfig(`integration-a-${suffix}`, database), () => Promise.resolve());
    const peerB = new PeerCouchDB(makeConfig(`integration-b-${suffix}`, database), () => Promise.resolve());
    const path = "note.md" as FilePathWithPrefix;
    const serviceLogs: string[] = [];
    setGlobalLogFunction((message) => serviceLogs.push(String(message)));

    try {
        await Promise.all([peerA.start(), peerB.start()]);
        assert(peerA.man.liveSyncLocalDB.isReady, "The first direct database should finish initialisation");
        assert(peerB.man.liveSyncLocalDB.isReady, "The second direct database should finish initialisation");

        const first: FileData = {
            ctime: 1_700_000_000_000,
            mtime: 1_700_000_000_000,
            size: 5,
            data: ["hello"],
        };
        assert(await peerA.put(path, first), "The first peer should store the file");

        const readByB = await peerB.get(path);
        assert(readByB !== false, "The second peer should read the file");
        assertEquals(readByB.data.join(""), "hello", "The second peer should receive the original content");

        const second: FileData = {
            ...first,
            mtime: first.mtime + 10_000,
            size: 7,
            data: ["updated"],
        };
        assert(await peerB.put(path, second), "The second peer should update the file");

        const readByA = await peerA.get(path);
        assert(readByA !== false, "The first peer should read the updated file");
        assertEquals(readByA.data.join(""), "updated", "The first peer should receive the updated content");

        assert(await peerB.delete(path), "The second peer should delete the file");
        assertEquals(await peerA.get(path), false, "The first peer should observe the deletion");

        const failedInitialisationLogs = serviceLogs.filter((message) =>
            message.includes("Failed to open KeyValueDB") ||
            message.includes("No replicator is available") ||
            message.includes("prevented the database from being ready")
        );
        assertEquals(
            failedInitialisationLogs.length,
            0,
            `Direct database initialisation should not run unrelated application services: ${failedInitialisationLogs.join("; ")}`,
        );
    } finally {
        await Promise.allSettled([peerA.stop(), peerB.stop()]);
        setGlobalLogFunction(defaultLogger);
        await (await requestDatabase(database, "DELETE")).body?.cancel();
    }
});
