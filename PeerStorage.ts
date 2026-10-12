import { PeerStorageConf, FileData, type Origin } from "./types.ts";
import { getDocData } from "@vrtmrz/livesync-commonlib/compat/common/utils";
import { isPlainText } from "@vrtmrz/livesync-commonlib/compat/string_and_binary/path";
import { parse, format, relative, resolve } from "@std/path";
import { format as posixFormat, parse as posixParse } from "@std/path/posix";
import { serialized } from "octagonal-wheels/concurrency/lock";
import { DispatchFun, Peer, PeerHealth } from "./Peer.ts";
import chokidar from "chokidar";
import { walk } from "fs/walk";
import { replaceStorageFile, StorageWriteProtocol } from "./StorageWriteProtocol.ts";

import { cancelTask, scheduleTask } from "octagonal-wheels/concurrency/task";
import { computeHash } from "./util.ts";
import { minimatch } from "minimatch";
import {
    Logger,
    LOG_LEVEL_INFO,
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_VERBOSE,
} from "octagonal-wheels/common/logger";

type PendingPath = { requested: number; since: number; attempts: number; reason: string; running?: Promise<void> };

export class PeerStorage extends Peer {
    declare config: PeerStorageConf;


    constructor(conf: PeerStorageConf, dispatcher: DispatchFun) {
        super(conf, dispatcher);
    }

    async delete(pathSrc: string, origin?: Origin): Promise<boolean> {
        return await this.section(pathSrc, () => new StorageWriteProtocol(this.config).locked(() => this.deleteLocked(pathSrc, origin)));
    }
    private async deleteLocked(pathSrc: string, origin?: Origin): Promise<boolean> {
        const lp = this.toLocalPath(pathSrc);
        const path = this.toStoragePath(lp);
        try {
            try {
                await Deno.stat(path);
            } catch (error) {
                if (error instanceof Deno.errors.NotFound) {
                    if (origin) await new StorageWriteProtocol(this.config).preserve(path, null, null, origin);
                    localStorage.removeItem(this._getKey(`file-stat-${lp}`));
                    return false;
                }
                throw error;
            }
            const protocol = new StorageWriteProtocol(this.config);
            await protocol.preserve(path, null, null, origin);
            await protocol.remove(path);
            try {
                await Deno.stat(path);
                throw new Error("Deleted file remains on disk");
            } catch (error) {
                if (!(error instanceof Deno.errors.NotFound)) throw error;
            }
            this.receiveLog(` ${path} deleted`);
            localStorage.removeItem(this._getKey(`file-stat-${lp}`));
        } catch (ex) {
            this.cache.set(lp, "");
            this.receiveLog(` ${path} delete failed`, LOG_LEVEL_NOTICE);
            Logger(ex, LOG_LEVEL_VERBOSE);
            throw ex;
        }
        this.runScript(path, true);
        return true;
    }
    async put(pathSrc: string, data: FileData, origin?: Origin): Promise<boolean> {
        return await this.section(pathSrc, () => new StorageWriteProtocol(this.config).locked(() => this.putLocked(pathSrc, data, origin)));
    }
    private async putLocked(pathSrc: string, data: FileData, origin?: Origin): Promise<boolean> {
        const lp = this.toLocalPath(pathSrc);
        const path = this.toStoragePath(lp);
        try {
            const bytes = data.data instanceof Uint8Array ? data.data : new TextEncoder().encode(getDocData(data.data));
            if (bytes.length === 0 && data.size > 0) throw new Error("Empty payload for a nonempty document");
            const previous = await Deno.readFile(path).catch((error) => {
                if (error instanceof Deno.errors.NotFound) return null;
                throw error;
            });
            if (previous && previous.length === bytes.length && previous.every((byte, index) => byte === bytes[index])) {
                if (origin) await new StorageWriteProtocol(this.config).preserve(path, bytes, data.mtime, origin);
                await this.writeFileStat(pathSrc);
                return false;
            }
            await new StorageWriteProtocol(this.config).preserve(path, bytes, data.mtime, origin);
            await replaceStorageFile(path, bytes, data.mtime);
            const accepted = await Deno.readFile(path);
            if (accepted.length !== bytes.length || !accepted.every((byte, index) => byte === bytes[index])) {
                throw new Error("Storage readback differs from the incoming bytes");
            }
            this.receiveLog(`${lp} saved`);
            await this.writeFileStat(pathSrc);
            this.runScript(path, false);
            return true;
        } catch (ex) {
            this.cache.set(lp, "");
            Logger(ex, LOG_LEVEL_INFO);
            this.receiveLog(`${lp} save failed`);
            throw ex;
        }
    }

    async runScript(filename: string, isDeleted: boolean): Promise<boolean> {
        if (!this.config.processor) return false;
        if (!this.config.processor.cmd) return false;

        try {
            const cmd = this.config.processor.cmd;
            const mode = isDeleted ? "deleted" : "modified";
            const args = this.config.processor.args.map(e => {
                if (e == "$filename") return filename;
                if (e == "$mode") return mode;
                return e
            });
            const scriptLineMessage = `Script: called ${cmd} with args ${JSON.stringify(args)}`;
            this.normalLog(`Processor : ${scriptLineMessage}`)
            const command = new Deno.Command(
                cmd, {
                args: args,
                cwd: ".",
                env: {
                    filename: filename,
                    mode: mode
                }
            });
            const { code, stdout, stderr } = await command.output();
            const stdoutText = new TextDecoder().decode(stdout);
            const stderrText = new TextDecoder().decode(stderr);
            if (code === 0) {
                this.normalLog("Processor called: Performed successfully.")
                this.normalLog(stdoutText);
            } else {
                this.normalLog("Processor called: Performed but with some errors.")
                this.normalLog(stderrText, LOG_LEVEL_NOTICE);
            }
            return true;
        } catch (ex) {
            this.normalLog("Processor: Error on processing");;
            this.normalLog(JSON.stringify(ex, null, 2));
            return false;
        }

    }

    async get(pathSrc: string): Promise<false | FileData> {
        const lp = this.toLocalPath(pathSrc);
        const path = this.toStoragePath(lp);
        const stat = await Deno.stat(path);
        if (!stat.isFile) {
            return false;
        }
        const ret: FileData = {
            ctime: stat.mtime?.getTime() ?? 0,
            mtime: stat.mtime?.getTime() ?? 0,
            size: stat.size,
            data: [],
        };
        if (isPlainText(path)) {
            ret.data = [await Deno.readTextFile(path)];
        } else {
            ret.data = await Deno.readFile(path);
        }
        return ret;
    }
    watcher?: chokidar.FSWatcher;

    private async closeChokidarWatcher(watcher?: chokidar.FSWatcher): Promise<void> {
        if (!watcher) return;
        try {
            await watcher.close();
        } catch (ex) {
            Logger(ex, LOG_LEVEL_VERBOSE);
        }
    }

    private epoch = 0;
    private run?: number;
    private readonly instance = crypto.randomUUID();
    private pending = new Map<string, PendingPath>();

    private section<T>(path: string, operation: () => Promise<T>): Promise<T> {
        return serialized(`storage-path:${this.toStoragePath(this.toLocalPath(path))}`, operation);
    }

    private relativePath(absolutePath: string): string {
        const path = this.toPosixPath(relative(this.toStoragePath(this.toLocalPath(".")), resolve(absolutePath)));
        if (path === ".." || path.startsWith("../")) throw new Error("Path is outside the storage root");
        return path || ".";
    }

    private async fileStat(path: string, data: FileData): Promise<string> {
        const protectedPath = this.config.writeProtocol?.protectedPaths.some(pattern => minimatch(path, pattern));
        return `${data.mtime}-${data.size}${protectedPath ? ":" + await computeHash(data.data) : ""}`;
    }

    private async forward(path: string, run?: number): Promise<void> {
        if (run !== undefined && run !== this.run) return;
        const snapshot = await new StorageWriteProtocol(this.config).locked(async () => {
            try { return await this.get(path); }
            catch (error) {
                if (!(error instanceof Deno.errors.NotFound)) throw error;
                const root = await Deno.stat(this.toStoragePath(this.toLocalPath(".")));
                if (!root.isDirectory) throw new Error("Storage root is unavailable");
                return null;
            }
        });
        const statKey = `file-stat-${this.toLocalPath(path)}`;
        const absentKey = this._getKey(`watch-pending-${path}`);
        if (run !== undefined && run !== this.run) return;
        if (snapshot === false) {
            const absolutePath = this.toStoragePath(this.toLocalPath(path));
            if (run !== undefined && (await Deno.stat(absolutePath)).isDirectory) await this.scanDirectory(absolutePath, run);
            return;
        }
        if (snapshot === null) {
            localStorage.setItem(absentKey, "1");
            this.sendLog(`${path} delete detected`);
            await this.dispatchToHub(this, this.toGlobalPath(path), false);
            if (run !== undefined && run !== this.run) return;
            localStorage.removeItem(this._getKey(statKey));
        } else {
            const stat = await this.fileStat(path, snapshot);
            if (run !== undefined && run !== this.run) return;
            if (this.getSetting(statKey) !== stat) {
                this.sendLog(`${path} change detected`);
                await this.dispatchToHub(this, this.toGlobalPath(path), snapshot);
                if (run !== undefined && run !== this.run) return;
                this.setSetting(statKey, stat);
            }
        }
        localStorage.removeItem(absentKey);
    }

    async dispatch(pathSrc: string): Promise<void> {
        const path = this.relativePath(pathSrc);
        await this.section(path, () => this.forward(path));
    }

    async dispatchDeleted(pathSrc: string): Promise<void> {
        await this.dispatch(pathSrc);
    }

    private timerKey(path: string, run: number): string {
        return `storage-watch:${this.instance}:${run}:${path}`;
    }

    private arm(path: string, run: number, wait: number): void {
        scheduleTask(this.timerKey(path, run), wait, () => this.attempt(path, run), true);
    }

    private notice(pathSrc: string, run: number): void {
        if (run !== this.run || pathSrc.split(/[\\/]/).at(-1)?.startsWith(".otask-tmp-")) return;
        try {
            const path = this.relativePath(pathSrc);
            let entry = this.pending.get(path);
            if (!entry) {
                entry = { requested: 0, since: Date.now(), attempts: 0, reason: "" };
                this.pending.set(path, entry);
            }
            entry.requested++;
            if (!entry.running) this.arm(path, run, 250);
        } catch (error) { Logger(error, LOG_LEVEL_NOTICE); }
    }

    private async attempt(path: string, run: number): Promise<void> {
        const entry = this.pending.get(path);
        if (run !== this.run || !entry || entry.running) return;
        const requested = entry.requested;
        const work = this.section(path, () => this.forward(path, run));
        entry.running = work;
        let wait = 250;
        try {
            await work;
            if (run !== this.run) return;
            entry.attempts = 0;
            entry.reason = "";
            if (entry.requested === requested) this.pending.delete(path);
        } catch (error) {
            if (run !== this.run) return;
            const reason = error instanceof Error ? error.message : String(error);
            if (entry.reason !== reason) this.sendLog(`${path} not synced: ${reason}`, LOG_LEVEL_NOTICE);
            entry.reason = reason;
            entry.attempts++;
            wait = Math.min(1000 * 2 ** Math.min(entry.attempts - 1, 6), 60000);
        } finally {
            entry.running = undefined;
            if (run === this.run && this.pending.get(path) === entry) this.arm(path, run, wait);
        }
    }

    toPosixPath(path: string) {
        const ret = posixFormat(parse(path));
        return ret;
    }
    toStoragePath(path: string) {
        const ret = resolve(format(posixParse(path)));
        return ret;
    }

    async writeFileStat(pathSrc: string) {
        const data = await this.get(pathSrc);
        if (data === false) return false;
        this.setSetting(`file-stat-${this.toLocalPath(pathSrc)}`, await this.fileStat(pathSrc, data));
    }

    async isChanged(pathSrc: string) {
        const data = await this.get(pathSrc);
        return data !== false && this.getSetting(`file-stat-${this.toLocalPath(pathSrc)}`) !== await this.fileStat(pathSrc, data);
    }

    watcherDeno?: Deno.FsWatcher;
    private watcherDrain?: Promise<void>;

    processFile(event: Deno.FsEvent) {
        if (this.run === undefined) return;
        if (event.flag === "rescan") this.notice(this.toStoragePath(this.toLocalPath(".")), this.run);
        for (const path of event.paths) this.notice(path, this.run);
    }

    private async scanDirectory(path: string, run: number): Promise<void> {
        for await (const entry of walk(path)) {
            if (run !== this.run) break;
            if (entry.isFile) this.notice(entry.path, run);
        }
    }

    private restoreAbsent(run: number): void {
        const prefix = this._getKey("watch-pending-");
        const paths: string[] = [];
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (key?.startsWith(prefix)) paths.push(key.substring(prefix.length));
        }
        for (const path of paths) this.notice(this.toStoragePath(this.toLocalPath(path)), run);
    }

    private async closeWatchers(): Promise<void> {
        const watcher = this.watcher;
        const watcherDeno = this.watcherDeno;
        const watcherDrain = this.watcherDrain;
        const pending = this.pending;
        const run = this.run;
        this.run = undefined;
        this.watcher = undefined;
        this.watcherDeno = undefined;
        this.watcherDrain = undefined;
        this.pending = new Map();
        if (run !== undefined) {
            for (const path of pending.keys()) cancelTask(this.timerKey(path, run));
        }
        if (watcherDeno) {
            try { watcherDeno.close(); }
            catch (error) { Logger(error, LOG_LEVEL_VERBOSE); }
        }
        await this.closeChokidarWatcher(watcher);
        await Promise.allSettled([
            ...(watcherDrain ? [watcherDrain] : []),
            ...[...pending.values()].flatMap(entry => entry.running ? [entry.running] : []),
        ]);
    }

    private async consumeDeno(watcher: Deno.FsWatcher, run: number): Promise<void> {
        try {
            for await (const event of watcher) {
                if (run !== this.run) break;
                this.processFile(event);
            }
        } catch (error) {
            if (run === this.run) Logger(error, LOG_LEVEL_NOTICE);
        } finally {
            if (this.watcherDeno === watcher) this.watcherDeno = undefined;
        }
    }

    async start(): Promise<void> {
        const run = ++this.epoch;
        await this.closeWatchers();
        if (run !== this.epoch) return;
        const root = this.toStoragePath(this.toLocalPath("."));
        this.run = run;
        if (this.config.useChokidar) {
            const watcher = chokidar.watch(root, {
                ignoreInitial: !this.config.scanOfflineChanges,
                ignored: (path: string) => path.split(/[\\/]/).at(-1)?.startsWith(".otask-tmp-") ?? false,
                awaitWriteFinish: { stabilityThreshold: 500 },
            });
            this.watcher = watcher;
            watcher.on("error", error => {
                if (run !== this.run) return;
                this.watcher = undefined;
                Logger(error, LOG_LEVEL_NOTICE);
                void this.closeChokidarWatcher(watcher);
            });
            for (const kind of ["add", "change", "unlink"] as const) {
                watcher.on(kind, path => this.notice(path, run));
            }
        } else {
            const watcher = Deno.watchFs(root, { recursive: true });
            this.watcherDeno = watcher;
            this.watcherDrain = this.consumeDeno(watcher, run);
        }
        this.restoreAbsent(run);
        if (!this.config.useChokidar && this.config.scanOfflineChanges) {
            await this.scanDirectory(root, run);
        }
    }

    async stop(): Promise<void> {
        ++this.epoch;
        await this.closeWatchers();
    }

    override health(): PeerHealth {
        const ok = !!(this.watcherDeno || this.watcher);
        const oldest = this.pending.entries().next().value;
        const detail = `${ok ? "watching" : "starting"}${oldest ? `; ${this.pending.size} pending, oldest ${oldest[0]} for ${Math.floor((Date.now() - oldest[1].since) / 1000)}s${oldest[1].reason ? ": " + oldest[1].reason : ""}` : ""}`;
        return { name: this.config.name, type: "storage", ok, detail, backendUp: true, restartWorthy: false };
    }
}
