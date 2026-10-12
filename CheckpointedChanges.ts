import { LOG_LEVEL_NOTICE, type LOG_LEVEL } from "octagonal-wheels/common/logger";

export type Change = PouchDB.Core.ChangesResponseChange<any>;

export interface ChangeCursor {
    // Called at the start of every pass; a rejection fails the pass, which is retried later.
    load(): Promise<string>;
    // Called only once every change up to and including `seq` was applied or deliberately skipped.
    save(seq: string): void;
}

const PAGE_SIZE = 100;
const FIRST_RETRY_MS = 1_000;
const LAST_RETRY_MS = 60_000;

function describe(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

// Follows a CouchDB changes feed one change at a time, in feed order, and moves the durable
// cursor only past changes whose application resolved. A change that fails holds the cursor
// where it was: later changes still apply, and a fresh pass from the held cursor retries it.
// Every pass and every live change runs on one promise chain, so applications never overlap.
export class CheckpointedChanges {
    watching = false;
    holding: string | undefined;
    applying: { change: string; since: number } | undefined;
    private generation = 0;
    private stopped = false;
    private queue: Promise<void> = Promise.resolve();
    private feed: PouchDB.Core.Changes<any> | undefined;
    private retryTimer: number | undefined;
    private retryDelay = FIRST_RETRY_MS;
    private readonly initialPassOrStop = Promise.withResolvers<void>();

    constructor(
        private readonly db: PouchDB.Database<any>,
        private readonly options: PouchDB.Core.ChangesOptions,
        private readonly apply: (change: Change) => Promise<void>,
        private readonly cursor: ChangeCursor,
        private readonly log: (message: string, level?: LOG_LEVEL) => void,
    ) {}

    start(): Promise<void> {
        this.restart();
        return this.initialPassOrStop.promise;
    }

    // Runs `task` on the same chain as change applications, after the work already queued.
    // A failure is logged; it does not hold the cursor, because the task is not a feed change.
    run(task: () => Promise<void>) {
        this.enqueue(async () => {
            if (!this.stopped) await task();
        });
    }

    // Resolves once the change being applied when it was called has finished.
    stop(): Promise<void> {
        this.stopped = true;
        this.generation++;
        this.closeFeed();
        clearTimeout(this.retryTimer);
        this.retryTimer = undefined;
        this.initialPassOrStop.resolve();
        return this.queue;
    }

    private restart() {
        const generation = ++this.generation;
        this.closeFeed();
        this.enqueue(() => this.pass(generation));
    }

    private closeFeed() {
        const feed = this.feed;
        this.feed = undefined;
        this.watching = false;
        feed?.cancel();
    }

    private enqueue(task: () => Promise<void>) {
        this.queue = this.queue.then(task).catch((error) => this.log(`Change processing failed: ${describe(error)}`, LOG_LEVEL_NOTICE));
    }

    private async pass(generation: number) {
        if (generation !== this.generation) return;
        this.holding = undefined;
        let since = "the saved cursor";
        try {
            since = await this.cursor.load();
            if (generation !== this.generation) return;
            for (;;) {
                const page = await this.db.changes({ ...this.options, since, limit: PAGE_SIZE });
                for (const change of page.results) await this.process(generation, change);
                if (generation !== this.generation) return;
                since = String(page.last_seq);
                this.acknowledge(generation, since);
                if (page.results.length < PAGE_SIZE) break;
            }
        } catch (error) {
            this.log(`Catching up from ${since} failed: ${describe(error)}`, LOG_LEVEL_NOTICE);
            this.retryLater(generation);
            return;
        }
        this.initialPassOrStop.resolve();
        this.follow(generation, since);
    }

    private follow(generation: number, since: string) {
        const feed = this.db.changes({ ...this.options, since, live: true });
        this.feed = feed;
        this.watching = true;
        feed.on("change", (change) => this.enqueue(() => this.process(generation, change)));
        feed.on("error", (error) => this.feedClosed(generation, feed, describe(error)));
        feed.on("complete", () => this.feedClosed(generation, feed, "completed"));
    }

    private feedClosed(generation: number, feed: PouchDB.Core.Changes<any>, reason: string) {
        if (generation !== this.generation || this.feed !== feed) return;
        this.feed = undefined;
        this.watching = false;
        this.log(`Changes feed closed (${reason}); resuming from the checkpoint`, LOG_LEVEL_NOTICE);
        this.retryLater(generation);
    }

    private async process(generation: number, change: Change) {
        if (generation !== this.generation) return;
        this.applying = { change: `${change.id} at ${change.seq}`, since: Date.now() };
        try {
            await this.apply(change);
        } catch (error) {
            if (generation !== this.generation) return;
            this.holding ??= `${change.id} at ${change.seq}: ${describe(error)}`;
            this.log(`Change ${change.id} at ${change.seq} was not applied; the checkpoint waits for it: ${describe(error)}`, LOG_LEVEL_NOTICE);
            this.retryLater(generation);
            return;
        } finally {
            this.applying = undefined;
        }
        this.acknowledge(generation, String(change.seq));
    }

    private acknowledge(generation: number, seq: string) {
        if (generation !== this.generation || this.holding !== undefined) return;
        this.cursor.save(seq);
        this.retryDelay = FIRST_RETRY_MS;
    }

    private retryLater(generation: number) {
        if (generation !== this.generation || this.retryTimer !== undefined) return;
        const delay = this.retryDelay;
        this.retryDelay = Math.min(delay * 2, LAST_RETRY_MS);
        this.retryTimer = setTimeout(() => {
            this.retryTimer = undefined;
            this.restart();
        }, delay);
    }
}
