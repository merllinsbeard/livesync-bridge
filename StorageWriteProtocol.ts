import { dirname, join, relative, resolve } from "@std/path";
import { minimatch } from "minimatch";
import { serialized } from "octagonal-wheels/concurrency/lock";
import type { Origin, PeerStorageConf } from "./types.ts";

async function syncDirectory(path: string) {
    const directory = await Deno.open(path, { read: true });
    try {
        await directory.sync();
    } finally {
        directory.close();
    }
}

export async function replaceStorageFile(path: string, bytes: Uint8Array, mtime?: number, mode = 0o644) {
    await Deno.mkdir(dirname(path), { recursive: true });
    const temporary = await Deno.makeTempFile({ dir: dirname(path), prefix: ".otask-tmp-" });
    try {
        const file = await Deno.open(temporary, { write: true, truncate: true });
        try {
            let offset = 0;
            while (offset < bytes.length) {
                const written = await file.write(bytes.subarray(offset));
                if (!written) throw new Error("Storage write made no progress");
                offset += written;
            }
            if (mtime !== undefined) await file.utime(new Date(mtime), new Date(mtime));
            await Deno.chmod(temporary, mode);
            await file.sync();
        } finally {
            file.close();
        }
        await Deno.rename(temporary, path);
        await syncDirectory(dirname(path));
    } finally {
        await Deno.remove(temporary).catch((error) => {
            if (!(error instanceof Deno.errors.NotFound)) throw error;
        });
    }
}

export class StorageWriteProtocol {
    constructor(private readonly config: PeerStorageConf) {}

    async locked<T>(operation: () => Promise<T>): Promise<T> {
        const protocol = this.config.writeProtocol;
        if (!protocol) return await operation();
        // Waiting flocks consume Deno's blocking pool, which the holder also needs for file I/O.
        return await serialized(`storage-write-protocol:${resolve(protocol.lockPath)}`, async () => {
            await Deno.mkdir(dirname(protocol.lockPath), { recursive: true });
            const lock = await Deno.open(protocol.lockPath, { write: true, create: true });
            try {
                await lock.lock(true);
                return await operation();
            } finally {
                lock.close();
            }
        });
    }

    async preserve(path: string, incoming: Uint8Array | null, sourceMtime: number | null, origin?: Origin) {
        const protocol = this.config.writeProtocol;
        if (!protocol) return;
        const localPath = relative(resolve(this.config.baseDir), resolve(path)).replaceAll("\\", "/");
        if (!protocol.protectedPaths.some((pattern) => minimatch(localPath, pattern))) return;
        const previous = await Deno.readFile(path).catch((error) => {
            if (error instanceof Deno.errors.NotFound) return null;
            throw error;
        });
        await preserveSyncInput(protocol.versionsPath, localPath, previous, incoming, sourceMtime, origin ? { origin } : {});
    }

    async remove(path: string) {
        await Deno.remove(path);
        await syncDirectory(dirname(path));
    }
}

export async function preserveSyncInput(root: string, path: string, previous: Uint8Array | null, incoming: Uint8Array | null, sourceMtime: number | null, extra: Record<string, unknown> = {}) {
        await Deno.mkdir(join(root, "blobs"), { recursive: true });
        await Deno.mkdir(join(root, "records"), { recursive: true });
        await syncDirectory(root);
        await syncDirectory(dirname(root));
        const hashes: (string | null)[] = [];
        for (const bytes of [previous, incoming]) {
            if (bytes === null) {
                hashes.push(null);
                continue;
            }
            const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes)));
            const hash = Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
            await replaceStorageFile(join(root, "blobs", `${hash}.bin`), bytes, undefined, 0o600);
            hashes.push(hash);
        }
        const id = crypto.randomUUID();
        const record = { id, kind: "sync_input", path, hash_before: hashes[0],
            hash_incoming: hashes[1], source_mtime: sourceMtime,
            received_at: new Date().toISOString(), side: "bridge", ...extra };
        await replaceStorageFile(join(root, "records", `${id}.json`),
            new TextEncoder().encode(JSON.stringify(record) + "\n"), undefined, 0o600);
}
