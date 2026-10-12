import { DirectFileManipulator } from "@vrtmrz/livesync-commonlib";
import {
    type DocumentID,
    type FilePathWithPrefix,
    MILESTONE_DOCID,
    type TweakValues,
} from "@vrtmrz/livesync-commonlib/compat/common/types";
import { PeerCouchDBConf, FileData, type CouchRevisions, type Origin } from "./types.ts";
import { decodeBinary } from "@vrtmrz/livesync-commonlib/compat/string_and_binary/convert";
import { isPlainText } from "@vrtmrz/livesync-commonlib/compat/string_and_binary/path";
import { DispatchFun, Peer, PeerHealth } from "./Peer.ts";
import {
    createBinaryBlob,
    createTextBlob,
    getDocData,
    isDocContentSame,
    unique,
} from "@vrtmrz/livesync-commonlib/compat/common/utils";
import { minimatch } from "minimatch";
import { promiseWithResolver } from "octagonal-wheels/promises";
import { serialized } from "octagonal-wheels/concurrency/lock";
import { LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";
import { type Change, CheckpointedChanges } from "./CheckpointedChanges.ts";
import { preserveSyncInput } from "./StorageWriteProtocol.ts";

type ManipulatorMetaEntry = Parameters<DirectFileManipulator["getByMeta"]>[0];
type ManipulatorReadyEntry = Awaited<ReturnType<DirectFileManipulator["getByMeta"]>>;
type EntryWithMeta = ManipulatorMetaEntry & { _id: DocumentID; _rev: string; _conflicts?: string[]; _revisions?: CouchRevisions };
type Base = NonNullable<Origin["base"]>;

const STUCK_APPLY_MS = 5 * 60_000;
const REMOTE_POLL_MS = 10_000;

// The changes the bridge follows. A deletion made without a body (an HTTP DELETE) has no `type`, and Mango's $ne
// never matches a missing field.
export const CHANGES_SELECTOR = { $or: [{ type: { $ne: "leaf" } }, { type: { $exists: false } }] };

// commonlib 0.1.19 tears a manipulator's managers down on its database's "close" event, which pouchdb-core 9 never
// emits (it emits "closed"), so close() alone leaves the managers' live changes feed running. Initialisation creates
// that feed and tears it down itself when it fails, so the teardown waits for initialisation to settle.
async function closeManipulator(man: DirectFileManipulator): Promise<void> {
    if (await man.ready.promise.then(() => true, () => false)) await man.liveSyncLocalDB.managers.teardownManagers();
    await man.close();
}

function isNoteEntry(doc: { type?: string }): boolean {
    return doc.type == "newnote" || doc.type == "plain";
}

function entryBytes(entry: ManipulatorReadyEntry): Uint8Array<ArrayBuffer> {
    return entry.type == "plain" ? new TextEncoder().encode(getDocData(entry.data)) : new Uint8Array(decodeBinary(entry.data));
}

function fileBytes(data: FileData): Uint8Array<ArrayBuffer> {
    return data.data instanceof Uint8Array ? data.data : new TextEncoder().encode(getDocData(data.data));
}

async function sha256(bytes: Uint8Array<ArrayBuffer>): Promise<string> {
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function entryBlob(entry: ManipulatorReadyEntry): Blob {
    return entry.type == "plain" ? createTextBlob(entry.data) : createBinaryBlob(new Uint8Array(decodeBinary(entry.data)));
}

function movedError(path: string, base: string | undefined, current: string | undefined) {
    return new Error(`${path} moved in CouchDB from ${base ?? "nothing"} to ${current ?? "nothing"} before this vault change was stored; the vault keeps its bytes until the newer revision arrives`);
}

export class PeerCouchDB extends Peer {
    man!: DirectFileManipulator;
    declare config: PeerCouchDBConf;
    private _started = promiseWithResolver<void>();
    private _connected = false;
    private _remoteEmpty = false;
    private _changes?: CheckpointedChanges;
    private _remoteTimer?: ReturnType<typeof setTimeout>;
    private _stopped = false;
    private _connectionAttempt?: Promise<void>;
    private _retiredManipulatorClosures = new Set<Promise<void>>();
    private _unrecorded = new Map<string, { rev: string; bytes: Uint8Array<ArrayBuffer> | null; mtime: number | null }>();
    constructor(conf: PeerCouchDBConf, dispatcher: DispatchFun) {
        super(conf, dispatcher);
        // The manipulator is built lazily in start(), only after a probe confirms
        // CouchDB is reachable. Building it here would start its one-shot init
        // against a possibly-down CouchDB, then discard and rebuild it on the
        // first successful connection.
    }
    // (Re)create the underlying DirectFileManipulator. Its constructor starts a
    // one-shot async database initialisation. Recovering from a failed attempt
    // requires a fresh manipulator because its `ready` promise remains settled.
    private _replaceManipulatorAndTrackClose(): void {
        const prev = this.man as DirectFileManipulator | undefined;
        this.man = new DirectFileManipulator(this.config, {
            // Bypass node:http compatibility shims for Deno, Traefik, and long-polling connections.
            fetch: (request, init) => globalThis.fetch(request, init),
        });
        if (prev) {
            const released = closeManipulator(prev);
            this._retiredManipulatorClosures.add(released);
            released.then(() => this._retiredManipulatorClosures.delete(released), (error) =>
                this.normalLog(`Closing the previous CouchDB connection failed: ${error instanceof Error ? error.message : error}`, LOG_LEVEL_NOTICE));
        }
    }
    // Resolves true when CouchDB stored the change, false when there was nothing to store,
    // and rejects when it was not stored, so the vault side does not record it as synced.
    async delete(pathSrc: string): Promise<boolean> {
        await this._started.promise;
        const path = this.toLocalPath(pathSrc);
        if (await this.isRepeating(pathSrc, false)) {
            return false;
        }
        try {
            if (this._isProtected(pathSrc)) return await this._withBasePathLock(pathSrc, () => this._deleteFromBase(pathSrc, path));
            const r = await this.man.delete(path);
            if (r) {
                this.receiveLog(` ${path} deleted`);
            } else {
                this.receiveLog(` ${path} delete failed`, LOG_LEVEL_NOTICE);
            }
            return r;
        } catch (error) {
            this.cache.set(pathSrc, "");
            throw error;
        }
    }
    async put(pathSrc: string, data: FileData): Promise<boolean> {
        await this._started.promise;
        const path = this.toLocalPath(pathSrc);
        if (await this.isRepeating(pathSrc, data)) {
            return false;
        }
        try {
            return await this._store(pathSrc, path, data);
        } catch (error) {
            this.cache.set(pathSrc, "");
            throw error;
        }
    }
    private async _store(pathSrc: string, path: string, data: FileData): Promise<boolean> {
        if (!this.man.liveSyncLocalDB.isTargetFile(path)) {
            this.receiveLog(` ${path} ignored`);
            return false;
        }
        const type = isPlainText(path) ? "plain" : "newnote";
        const info = {
            ctime: data.ctime,
            mtime: data.mtime,
            size: data.size
        };
        const saveData = (data.data instanceof Uint8Array) ? createBinaryBlob(data.data) : createTextBlob(data.data);
        if (this._isProtected(pathSrc)) return await this._withBasePathLock(pathSrc, () => this._storeOnBase(pathSrc, path, saveData, fileBytes(data), info, type));
        const old = await this.man.get(path as FilePathWithPrefix, true) as false | ManipulatorMetaEntry;
        // const old = await this.getMeta(path as FilePathWithPrefix);
        if (old && Math.abs(this.compareDate(info, old)) < 3600) {
            const oldDoc = await this.man.getByMeta(old);
            if (oldDoc && ("data" in oldDoc)) {
                const d = oldDoc.type == "plain" ? createTextBlob(oldDoc.data) : createBinaryBlob(new Uint8Array(decodeBinary(oldDoc.data)));
                if (await isDocContentSame(d, saveData)) {
                    this.normalLog(` Skipped (Same) ${path} `);
                    return false;
                }
            }
        }
        if (!await this.man.put(path, saveData, info, type)) throw new Error(`${path} was not stored in CouchDB`);
        this.receiveLog(` ${path} saved`);
        return true;
    }
    private async _storeOnBase(pathSrc: string, path: string, data: Blob, bytes: Uint8Array<ArrayBuffer>, info: Pick<FileData, "ctime" | "mtime" | "size">, type: "plain" | "newnote"): Promise<boolean> {
        const id = await this.man.path2id(path as FilePathWithPrefix);
        const current = await this._currentDoc(id);
        await this._recordPendingAncestor(pathSrc, current);
        if (current && !current.deleted && isNoteEntry(current) && await isDocContentSame(entryBlob(await this.man.getByMeta(current)), data)) {
            this.normalLog(` Skipped (Same) ${path} `);
            await this._recordAndConfirmBase(pathSrc, current._rev, bytes, info.mtime);
            return false;
        }
        const base = this._base(pathSrc);
        if (current && base === undefined) throw this._queuedWinnerPullError(pathSrc, path, id, current._rev);
        const parent = current === undefined && base?.sha256 === null ? undefined : base?.rev;
        const stored = await this.man.liveSyncLocalDB.putDBEntryWithLiveBaseRevision({
            _id: id, path: path as FilePathWithPrefix, data, ...info, type, datatype: type, eden: {}, children: [],
        }, parent as string);
        if (stored) {
            await this._recordAndConfirmBase(pathSrc, stored.rev, bytes, info.mtime);
            this.receiveLog(` ${path} saved`);
            return true;
        }
        const now = (await this._currentDoc(id))?._rev;
        if (now !== parent) throw movedError(path, base?.rev, now);
        throw new Error(`${path} was not stored in CouchDB`);
    }
    private async _deleteFromBase(pathSrc: string, path: string): Promise<boolean> {
        const id = await this.man.path2id(path as FilePathWithPrefix);
        const current = await this._currentDoc(id);
        if (!current) return false;
        await this._recordPendingAncestor(pathSrc, current);
        if (current.deleted) {
            await this._recordAndConfirmBase(pathSrc, current._rev, null, current.mtime);
            return false;
        }
        const base = this._base(pathSrc);
        if (base === undefined) throw this._queuedWinnerPullError(pathSrc, path, id, current._rev);
        if (current._rev !== base.rev) throw movedError(path, base.rev, current._rev);
        const mtime = Date.now();
        let stored: PouchDB.Core.Response;
        try {
            stored = await this.man.liveSyncLocalDB.putRaw({ ...current, deleted: true, mtime });
        } catch (error) {
            if ((error as { status?: number })?.status === 409) throw movedError(path, base.rev, (await this._currentDoc(id))?._rev);
            throw error;
        }
        await this._recordAndConfirmBase(pathSrc, stored.rev, null, mtime);
        this.receiveLog(` ${path} deleted`);
        return true;
    }
    private _queuedWinnerPullError(pathSrc: string, path: string, id: DocumentID, current: string): Error {
        this._changes?.run(() => this._bringProtected(pathSrc, id));
        return new Error(`${path} has no verified base revision; CouchDB keeps ${current}, which is delivered to the vault before a vault change can be stored`);
    }
    async get(pathSrc: FilePathWithPrefix): Promise<false | FileData> {
        await this._started.promise;
        const path = this.toLocalPath(pathSrc) as FilePathWithPrefix;
        const ret = await this.man.get(path) as false | ManipulatorReadyEntry;
        if (ret === false) {
            return false;
        }
        return {
            ctime: ret.ctime,
            mtime: ret.mtime,
            data: ret.type == "newnote" ? new Uint8Array(decodeBinary(ret.data)) : ret.data,
            size: ret.size,
            deleted: ret.deleted
        };
    }
    async getMeta(pathSrc: FilePathWithPrefix): Promise<false | FileData> {
        await this._started.promise;
        const path = this.toLocalPath(pathSrc) as FilePathWithPrefix;
        const ret = await this.man.get(path, true) as false | ManipulatorMetaEntry;
        if (ret === false) {
            return false;
        }
        return {
            ctime: ret.ctime,
            mtime: ret.mtime,
            data: [],
            size: ret.size,
            deleted: ret.deleted
        };
    }
    // Probe CouchDB the same way PouchDB will: a request whose body must parse as
    // JSON. A half-ready CouchDB (or a proxy error page) returns a non-JSON body —
    // exactly the case that used to crash the bridge — so we treat it as "not
    // ready yet" and let the caller retry, fast, instead of waiting on a hung init.
    private async _probeCouch(timeoutMs = 10000): Promise<void> {
        // Read straight from config (the manipulator may not be built yet, and these
        // are the same credentials it will use).
        const url = `${this.config.url}/${this.config.database}`;
        const headers: Record<string, string> = {};
        if (this.config.username) {
            // UTF-8-safe Basic auth: btoa() throws on code points > 0xFF, so a
            // non-ASCII password would otherwise make every probe fail forever even
            // though CouchDB would accept it.
            const creds = `${this.config.username}:${this.config.password ?? ""}`;
            const b64 = btoa(String.fromCharCode(...new TextEncoder().encode(creds)));
            headers["Authorization"] = `Basic ${b64}`;
        }
        // Bounded so a hung connection can't stall either the connect loop or the
        // heartbeat's reachability check (which would look like a wedged process).
        const res = await globalThis.fetch(url, { headers, signal: AbortSignal.timeout(timeoutMs) });
        if (!res.ok) {
            await res.body?.cancel();
            throw new Error(`CouchDB not ready: HTTP ${res.status}`);
        }
        await res.json();
    }

    // Is CouchDB up and serving right now? Uses the same success threshold as the
    // connect probe (200 + parseable JSON), so a still-warming-up CouchDB (refused,
    // or 503) reads as down — we don't want a restart while it boots.
    private async _couchReachable(): Promise<boolean> {
        try {
            await this._probeCouch(5000);
            return true;
        } catch {
            return false;
        }
    }

    // Bound the manipulator's one-shot initialisation so a transport which accepts
    // the request but never answers becomes a retriable failure.
    private _waitReady(timeoutMs: number): Promise<void> {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timeout = new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new Error("CouchDB init timed out")), timeoutMs);
        });
        return Promise.race([this.man.ready.promise as Promise<void>, timeout]).finally(() => {
            if (timer !== undefined) clearTimeout(timer);
        });
    }

    async start(): Promise<void> {
        let attempt = 0;
        // Supervised connect loop. CouchDB may be unreachable or still warming up
        // (classically right after a host reboot, when CouchDB and the bridge start
        // together). Rather than letting a failed connect surface as a fatal
        // unhandled rejection — which crash-looped the bridge into systemd's start
        // limit and left it down for days — retry with capped backoff until CouchDB
        // answers, then begin watching.
        for (;;) {
            try {
                await this._probeCouch();
                if (this._stopped) return;
                // CouchDB answered cleanly; rebuild against it so init starts fresh.
                this._replaceManipulatorAndTrackClose();
                await (this._connectionAttempt = this._connectAndWatch());
                if (attempt > 0) {
                    this.normalLog(`Connected to CouchDB after ${attempt} retr${attempt === 1 ? "y" : "ies"}.`, LOG_LEVEL_NOTICE);
                }
                this._connected = true;
                this._started.resolve();
                return;
            } catch (e) {
                attempt++;
                const delay = Math.min(30000, 1000 * 2 ** Math.min(attempt - 1, 5));
                this.normalLog(`CouchDB connect attempt ${attempt} failed; retrying in ${delay / 1000}s.`, LOG_LEVEL_NOTICE);
                this.debugLog(`${e instanceof Error ? (e.stack ?? e.message) : e}`);
                await new Promise((r) => setTimeout(r, delay));
            }
        }
    }

    private async _connectAndWatch(): Promise<void> {
        {
            const baseDir = this.toLocalPath("");
            await this._waitReady(15000);
            const w = await this.man.rawGet<Record<string, any>>(MILESTONE_DOCID);
            if (w && "tweak_values" in w) {
                if (this.config.useRemoteTweaks) {
                    const tweaks = Object.values(w["tweak_values"])[0] as TweakValues;
                    // console.log(tweaks)
                    const orgConf = { ...this.config } as Record<string, any>;
                    this.config.customChunkSize = tweaks.customChunkSize ?? this.config.customChunkSize;
                    this.config.minimumChunkSize = tweaks.minimumChunkSize ?? this.config.minimumChunkSize;
                    if (tweaks.encrypt && !this.config.passphrase) {
                        throw new Error("Remote database is encrypted but no passphrase provided.");
                    }
                    if (tweaks.usePathObfuscation && !this.config.obfuscatePassphrase) {
                        throw new Error("Remote database is obfuscated but no obfuscate passphrase provided.");
                    }
                    this.config.hashAlg = tweaks.hashAlg ?? this.config.hashAlg;
                    this.config.maxAgeInEden = tweaks.maxAgeInEden ?? this.config.maxAgeInEden;
                    this.config.maxTotalLengthInEden = tweaks.maxTotalLengthInEden ?? this.config.maxTotalLengthInEden;
                    this.config.maxChunksInEden = tweaks.maxChunksInEden ?? this.config.maxChunksInEden;
                    this.config.useEden = tweaks.useEden ?? this.config.useEden;
                    if (!this.config.enableCompression != !tweaks.enableCompression) {
                        throw new Error("Compression setting mismatched.");
                    }
                    this.config.useDynamicIterationCount = tweaks.useDynamicIterationCount ?? this.config.useDynamicIterationCount;
                    this.config.enableChunkSplitterV2 = tweaks.enableChunkSplitterV2 ?? this.config.enableChunkSplitterV2;
                    this.config.chunkSplitterVersion = tweaks.chunkSplitterVersion ?? this.config.chunkSplitterVersion;
                    this.config.E2EEAlgorithm = tweaks.E2EEAlgorithm ?? this.config.E2EEAlgorithm;
                    this.config.minimumChunkSize = tweaks.minimumChunkSize ?? this.config.minimumChunkSize;
                    this.config.customChunkSize = tweaks.customChunkSize ?? this.config.customChunkSize;
                    this.config.doNotUseFixedRevisionForChunks = tweaks.doNotUseFixedRevisionForChunks ?? this.config.doNotUseFixedRevisionForChunks;
                    this.config.handleFilenameCaseSensitive = tweaks.handleFilenameCaseSensitive ?? this.config.handleFilenameCaseSensitive;
                    const newConf = { ...this.config } as Record<string, any>;
                    this.man.options = this.config;
                    // Re-initialisation replaces the managers without tearing down the current ones (see closeManipulator).
                    await this.man.liveSyncLocalDB.managers.teardownManagers();
                    await this.man.liveSyncLocalDB.initializeDatabase()
                    // await this.man.managers.initManagers();
                    const diff = unique([...Object.keys(orgConf), ...Object.keys(tweaks)]).filter(k => orgConf[k] != newConf[k]);
                    if (diff.length > 0) {
                        this.normalLog(`Remote tweaks changed --->`);
                        for (const diffKey of diff) {
                            this.normalLog(`${diffKey}\t: ${orgConf[diffKey]} \t : ${newConf[diffKey]}`);
                        }
                        this.normalLog(`<--- Remote tweaks changed`);
                    }
                }
            }
            if (!w) {
                this.normalLog(`Remote database looks like empty. fetch from the first.`);
                this.setSetting("remote-created", "0");
                // Connected fine; there's just nothing to watch yet. Mark it so health
                // counts this as syncing rather than a stuck "not watching" state.
                this._remoteEmpty = true;
                this._waitForRemote();
                return;
            }
            this._remoteEmpty = false;
            await this._changes?.stop();
            if (this._stopped) return;
            this._changes = new CheckpointedChanges(
                this.man.liveSyncLocalDB.localDatabase,
                { include_docs: true, conflicts: true, selector: CHANGES_SELECTOR },
                (change) => this._apply(change, baseDir),
                { load: () => this._passCursor(), save: (seq) => this.setSetting("since", seq) },
                (message, level) => this.normalLog(message, level),
            );
            await this._changes.start();
        }
    }
    private _waitForRemote() {
        clearTimeout(this._remoteTimer);
        if (this._stopped) return;
        this._remoteTimer = setTimeout(() => {
            this._remoteTimer = undefined;
            if (this._stopped) return;
            (this._connectionAttempt = this._connectAndWatch()).catch((error) => {
                this.normalLog(`Looking for the LiveSync milestone failed: ${error instanceof Error ? error.message : error}`, LOG_LEVEL_NOTICE);
                this._waitForRemote();
            });
        }, REMOTE_POLL_MS);
    }
    private async _passCursor(): Promise<string> {
        const milestone = await this.man.rawGet<Record<string, any>>(MILESTONE_DOCID);
        if (!milestone) throw new Error("the LiveSync milestone is missing; the remote is being rebuilt or was removed");
        await this._ensureCursorForRemoteGeneration(`${milestone.created}`);
        return this.getSetting("since")!;
    }
    private async _ensureCursorForRemoteGeneration(created: string): Promise<void> {
        if (this.getSetting("remote-created") !== created) {
            this.normalLog(`Remote database looks like rebuilt. fetch from the first again.`);
            this.setSetting("since", "0");
            this.setSetting("remote-created", created);
            return;
        }
        const saved = this.getSetting("since");
        if (saved && saved !== "now") {
            this.normalLog(`Watch starting from ${saved}`);
            return;
        }
        const seed = this.config.writeProtocol?.initialSince;
        const since = seed ?? `${(await this.man.liveSyncLocalDB.localDatabase.info()).update_seq}`;
        this.normalLog(seed
            ? `Watch starting from the configured initialSince ${seed}`
            : `No saved cursor and no initialSince: watch starting from the current head ${since}; earlier changes are not replayed`,
            LOG_LEVEL_NOTICE);
        this.setSetting("since", since);
    }
    private _isInterested(docPath: string, baseDir: string): boolean {
        if (docPath.indexOf(":") !== -1) {
            if (this.config.includeInternal && docPath.startsWith("i:")) {
                const stripped = docPath.substring(2);
                return this.config.includeInternal.some(pattern => minimatch(stripped, pattern, { dot: true }));
            }
            return false;
        }
        return docPath.startsWith(baseDir);
    }
    private _isProtected(path: string): boolean {
        return this.config.writeProtocol?.protectedPaths.some((pattern) => minimatch(path, pattern)) ?? false;
    }
    private _base(path: string): Base | undefined {
        const saved = this.getSetting(`rev-${path}`);
        return saved ? JSON.parse(saved) as Base : undefined;
    }
    private _withBasePathLock<T>(path: string, operation: () => Promise<T>): Promise<T> {
        return serialized(`couch-base:${this.config.name}:${path}`, operation);
    }
    private async _setBase(path: string, rev: string, bytes: Uint8Array<ArrayBuffer> | null) {
        this.setSetting(`rev-${path}`, JSON.stringify({ rev, sha256: bytes && await sha256(bytes) } satisfies Base));
    }
    private async _recordAndConfirmBase(path: string, rev: string, bytes: Uint8Array<ArrayBuffer> | null, mtime: number | null) {
        const confirmed: Base = { rev, sha256: bytes && await sha256(bytes) };
        const base = this._base(path);
        if (base?.rev === confirmed.rev && base.sha256 === confirmed.sha256) return;
        const versionsPath = this.config.writeProtocol?.versionsPath;
        if (versionsPath) {
            this._unrecorded.set(path, { rev, bytes, mtime });
            await preserveSyncInput(versionsPath, path, null, bytes, mtime, { kind: "couch_base", peer: this.config.name, rev });
        }
        this._unrecorded.delete(path);
        this.setSetting(`rev-${path}`, JSON.stringify(confirmed));
    }
    private async _recordPendingAncestor(path: string, doc: EntryWithMeta | undefined) {
        const own = this._unrecorded.get(path);
        if (!own || !doc) return;
        const { start, ids } = doc._revisions ?? { start: 0, ids: [] };
        const ancestry = doc._revisions ? ids.map((id, index) => `${start - index}-${id}`) : [doc._rev];
        if (ancestry.includes(own.rev)) await this._recordAndConfirmBase(path, own.rev, own.bytes, own.mtime);
    }
    private async _currentDoc(id: string, options: PouchDB.Core.GetOptions = {}) {
        try {
            return await this.man.liveSyncLocalDB.getRaw<EntryWithMeta>(id as DocumentID, options);
        } catch (error) {
            if ((error as { status?: number })?.status === 404) return undefined;
            throw error;
        }
    }
    private async _apply(change: Change, baseDir: string): Promise<void> {
        const doc = change.doc as (EntryWithMeta & { _deleted?: boolean }) | undefined;
        if (doc?._deleted) return await this._bringTombstone(doc, baseDir);
        if (!doc || !isNoteEntry(doc)) return;
        const path = this._vaultPath(doc, baseDir);
        if (path === undefined) return;
        if (!this._isProtected(path)) {
            await this._deliver(path, await this.man.getByMeta(doc));
            return;
        }
        await this._bringProtected(path, doc._id);
    }
    private _vaultPath(doc: EntryWithMeta, baseDir: string): string | undefined {
        if (!this._isInterested(doc.path, baseDir)) return undefined;
        if (!this.man.liveSyncLocalDB.isTargetFile(this.man.$$id2path(doc._id, doc))) return undefined;
        let path = doc.path.substring(baseDir.length);
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.startsWith("i:")) {
            path = path.substring(2);
        }
        return path;
    }
    private async _bringProtected(path: string, id: DocumentID) {
        const before = this.getSetting(`rev-${path}`);
        const current = await this._currentDoc(id, { conflicts: true, revs: true });
        if (!current || !isNoteEntry(current)) return;
        await this._preserveConflicts(path, current);
        await this._deliverWinnerOnUnchangedBase(path, current, await this.man.getByMeta(current), before);
    }
    // A hard deletion (`_deleted`) hides the document from a plain read; LiveSync keeps the body in the tombstone,
    // other clients leave it bodiless, so the file is then found through the tombstone's parent.
    private async _bringTombstone(doc: { _id: DocumentID; _rev: string; type?: string }, baseDir: string) {
        if (doc._id.startsWith("h:") || doc.type == "leaf") return;
        const tombstone = await this._currentDoc(doc._id, { rev: doc._rev, revs: true });
        if (!tombstone?._revisions) return;
        const path = isNoteEntry(tombstone) ? this._vaultPath(tombstone, baseDir) : await this._tombstonePath(tombstone, baseDir);
        if (path === undefined) return;
        if (!this._isProtected(path)) return await this.dispatch(path, false);
        const before = this.getSetting(`rev-${path}`);
        if (await this._currentDoc(doc._id)) return;
        await this._deliverWinnerOnUnchangedBase(path, tombstone, undefined, before);
    }
    private async _tombstonePath(tombstone: EntryWithMeta, baseDir: string): Promise<string | undefined> {
        const { start, ids } = tombstone._revisions!;
        if (ids.length > 1) {
            const parent = await this._currentDoc(tombstone._id, { rev: `${start - 1}-${ids[1]}` });
            if (parent) return isNoteEntry(parent) ? this._vaultPath(parent, baseDir) : undefined;
        }
        const prefix = this._getKey("rev-");
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (!key?.startsWith(prefix)) continue;
            const path = key.substring(prefix.length);
            if (await this.man.path2id(this.toLocalPath(path) as FilePathWithPrefix) === tombstone._id) return path;
        }
        this.normalLog(`Deleted document ${tombstone._id} at ${tombstone._rev} maps to no known file; the vault is left as it is`, LOG_LEVEL_NOTICE);
        return undefined;
    }
    private async _deliverWinnerOnUnchangedBase(path: string, doc: EntryWithMeta, entry: ManipulatorReadyEntry | undefined, before: string | null) {
        const stored = await this._withBasePathLock(path, async () => {
            if (this.getSetting(`rev-${path}`) !== before) return undefined;
            await this._recordPendingAncestor(path, doc);
            return this.getSetting(`rev-${path}`);
        });
        if (stored === undefined) return;
        const base = stored ? JSON.parse(stored) as Base : null;
        if (base?.rev === doc._rev) return;
        const origin: Origin = { peer: this.config.name, rev: doc._rev, revisions: doc._revisions!, winner: doc._rev, base };
        if (entry === undefined) {
            this.sendLog(`${path} delete detected`);
            await this.dispatch(path, false, origin);
        } else {
            await this._deliver(path, entry, origin);
        }
        const bytes = entry === undefined || entry.deleted || entry._deleted ? null : entryBytes(entry);
        await this._withBasePathLock(path, async () => {
            if (this.getSetting(`rev-${path}`) === stored) await this._setBase(path, doc._rev, bytes);
        });
    }
    private async _preserveConflicts(path: string, current: EntryWithMeta) {
        for (const rev of current._conflicts ?? []) {
            const key = `conflict-${current._id}-${rev}`;
            if (this.getSetting(key)) continue;
            const leafDoc = await this.man.liveSyncLocalDB.getRaw<EntryWithMeta>(current._id, { rev, revs: true });
            const leaf = await this.man.getByMeta(leafDoc);
            this.normalLog(`${path}: conflicting revision ${rev} stays in CouchDB beside winner ${current._rev}`, LOG_LEVEL_NOTICE);
            const versionsPath = this.config.writeProtocol?.versionsPath;
            if (versionsPath) {
                const origin: Origin = { peer: this.config.name, rev, revisions: leafDoc._revisions!, winner: current._rev, base: this._base(path) ?? null };
                await preserveSyncInput(versionsPath, path, null, entryBytes(leaf), leaf.mtime, { origin });
            }
            this.setSetting(key, current._rev);
        }
    }
    private async _deliver(path: string, entry: ManipulatorReadyEntry, origin?: Origin) {
        if (entry.deleted || entry._deleted) {
            this.sendLog(`${path} delete detected`);
            await this.dispatch(path, false, origin);
            return;
        }
        const data = entry.type == "plain" ? entry.data : new Uint8Array(decodeBinary(entry.data));
        this.sendLog(`${path} change detected`);
        await this.dispatch(path, { ctime: entry.ctime, mtime: entry.mtime, size: entry.size, deleted: false, data }, origin);
    }
    async dispatch(path: string, data: FileData | false, origin?: Origin) {
        if (await this.isRepeating(path, data) && !origin) return;
        try {
            await this.dispatchToHub(this, this.toGlobalPath(path), data, origin);
        } catch (error) {
            this.cache.set(path, "");
            throw error;
        }
    }
    async stop(): Promise<void> {
        this._stopped = true;
        clearTimeout(this._remoteTimer);
        await this._changes?.stop();
        await Promise.allSettled(this._connectionAttempt ? [this._connectionAttempt] : []);
        if (this.man) await closeManipulator(this.man);
        await Promise.all(this._retiredManipulatorClosures);
    }
    override health(): PeerHealth {
        const watching = this._changes?.watching === true;
        const holding = this._changes?.holding;
        const applying = this._changes?.applying;
        const stuck = applying && Date.now() - applying.since > STUCK_APPLY_MS ? applying.change : undefined;
        const syncing = this._connected && ((watching && !holding && !stuck) || this._remoteEmpty);
        return {
            name: this.config.name,
            type: "couchdb",
            ok: syncing,
            detail: !this._connected ? "connecting" : stuck ? `stuck applying ${stuck}` : holding ? `retrying ${holding}` : (watching ? "watching" : (this._remoteEmpty ? "connected (empty remote)" : "reconnecting")),
            backendUp: syncing,
            restartWorthy: false,
        };
    }
    // Backend reachability for the base restart logic — probe CouchDB (bounded). The
    // base only calls this once a peer has been healthy and is now failing, so a
    // CouchDB outage (probe fails → backendUp false) keeps the peer non-restart-worthy.
    override checkBackendUp(): Promise<boolean> {
        return this._couchReachable();
    }
}
