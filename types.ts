import type { DirectFileManipulatorOptions } from "@vrtmrz/livesync-commonlib";

export interface Config {
    peers: PeerConf[];
}
export type PeerConf = PeerStorageConf | PeerCouchDBConf;

export type CouchRevisions = { start: number; ids: string[] };
export type Origin = {
    peer: string;
    rev: string;
    revisions: CouchRevisions;
    winner: string;
    base: { rev: string; sha256: string | null } | null;
};
export type Delivery = Origin & {
    // Runs `write` only while the vault's verified base is still the one this revision was checked against, and then
    // makes this revision the base. Once the vault has confirmed another revision, it resolves false without running
    // `write`, and the CouchDB peer reads its winner again.
    apply(write: () => Promise<boolean>): Promise<boolean>;
};

export interface PeerStorageConf {
    scanOfflineChanges?: boolean;
    type: "storage";
    group?: string;
    name: string;
    baseDir: string;
    processor?: {
        cmd: string,
        args: string[]
    }
    useChokidar?: boolean;
    writeProtocol?: {
        lockPath: string;
        versionsPath: string;
        protectedPaths: string[];
    };
}
export interface PeerCouchDBConf extends DirectFileManipulatorOptions {
    /** Glob patterns for i:-prefixed (internal) files to sync, e.g. [".claude/**"] */
    includeInternal?: string[];
    writeProtocol?: {
        protectedPaths: string[];
        versionsPath?: string;
        initialSince?: string;
    };
    type: "couchdb";
    useRemoteTweaks?: true;
    group?: string;
    name: string;
    database: string;
    username: string;
    password: string;
    url: string;
    customChunkSize?: number;
    minimumChunkSize?: number;
    passphrase: string;
    obfuscatePassphrase: string;
    baseDir: string;
}



export function isCouchDBPeer(peer: PeerConf): peer is PeerCouchDBConf {
    return peer.type == "couchdb";
}

export function isStoragePeer(peer: PeerConf): peer is PeerStorageConf {
    return peer.type == "storage";
}

export type FileData = {
    ctime: number;
    mtime: number;
    size: number;
    data: string[] | Uint8Array<ArrayBuffer>;
    deleted?: boolean;
}
