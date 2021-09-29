import * as crypto from "crypto";
import * as pg from "pg";
import { parse } from "pg-connection-string";

import { assertNever } from "../../node_modules/assert-never/index";
import { AsyncTrigger } from "../ConcurrencyUtils.ts/AsyncTrigger";
import { performanceMeasure } from "../Debug/performance_timing";
import { withConnectPg } from "../pg_extra";
import { PostgresCluster, PostgresClusterCollection } from "../PostgresClusterUtils/PostgresClusterCollection";
import { DatabaseTemplateHash, DatabaseTemplateMap } from "./DatabaseTemplateMap";

export type PostgresUrl = string;

const MAX_READY_CLONES = 24;

const WAIT_FOR_COMMIT_TIMEOUT = 20000;

export class TemporaryDatabaseService {
    constructor() {
        templateClonerWorker(this.templateClonerWorkerRunningCancellationToken, this.templates, this.asyncTrigger).finally(() => {
            this.templateClonerWorkerRunning = false;
        });
    }

    async getStatus(): Promise<ClusterStatus[]> {
        const clusters = await this.postgresClusterCollection.getPostgresClusterUrls();
        const results: ClusterStatus[] = [];

        for (const cluster of clusters) {
            results.push({
                postgresVersion: cluster.postgresVersion,
                masterUrl: cluster.url,
                databases: []
            });
        }

        function lookupPostgres(postgresVersion: string): ClusterStatus {
            for (const result of results) {
                if (result.postgresVersion === postgresVersion) {
                    return result;
                }
            }
            throw new Error(`The Impossible happened: ${postgresVersion} not found`);
        }

        this.templates.forEach((value, postgresVersion, hash) => {
            switch (value.type) {
                case "TemplateReady":
                    lookupPostgres(postgresVersion).databases.push({
                        hash: hash,
                        templateDb: value.templateDb,
                        pending: null,
                        readyClones: value.readyClones
                    });
                    break;
                case "WaitingForCommit":
                    lookupPostgres(postgresVersion).databases.push({
                        hash: hash,
                        templateDb: null,
                        pending: [],
                        readyClones: []
                    });
                    break;
                default: assertNever(value);
            }
        });

        this.commitTokens.forEach((value, commitToken) => {
            const cluster = lookupPostgres(value.postgresVersion);

            // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
            function getDatabase() {
                for (const database of cluster.databases) {
                    if (database.hash === value.hash) {
                        return database;
                    }
                }
                const newDatabase = {
                    hash: value.hash,
                    templateDb: null,
                    pending: [],
                    readyClones: []
                };
                cluster.databases.push(newDatabase);
                return newDatabase;
            }

            const database = getDatabase();
            if (database.pending === null) {
                database.pending = [];
            }
            database.pending.push({
                registeredAt: value.registeredAt,
                commitToken: commitToken,
                templateDb: value.templateDb
            });
        });

        return results;
    }

    async getFreshDatabase(postgresVersion: string, hash: DatabaseTemplateHash): Promise<FreshDatabaseResult> {
        const existing = this.templates.get(postgresVersion, hash);
        if (existing !== undefined) {
            switch (existing.type) {
                case "TemplateReady": {
                    if (existing.readyClones.length > 0) {
                        const first = existing.readyClones[0];
                        existing.readyClones.shift();
                        this.asyncTrigger.triggerChange();
                        return {
                            type: "Ready",
                            url: urlReplaceDbName(existing.masterUrl, first)
                        };
                    } else {
                        await this.asyncTrigger.waitForChange();
                        return await this.getFreshDatabase(postgresVersion, hash);
                    }
                }
                case "WaitingForCommit": {
                    // Wait for the other concurrent client to commit, so
                    // we can use its result. But we need a time limit in
                    // case the concurrent client crashed.

                    const waitingFor = (new Date().getTime()) - (existing.registeredAt.getTime());
                    const timeLeft = WAIT_FOR_COMMIT_TIMEOUT - waitingFor;

                    if (timeLeft <= 0) {
                        return await this.createTemplate(postgresVersion, hash);
                    } else {
                        const timeoutHandler = setTimeout(() => {
                            this.asyncTrigger.triggerChange();
                        }, timeLeft + 500);  // Add a small margin
                        await this.asyncTrigger.waitForChange();
                        clearTimeout(timeoutHandler);
                        return await this.getFreshDatabase(postgresVersion, hash);
                    }
                }
                default: return assertNever(existing);
            }
        } else {
            return await this.createTemplate(postgresVersion, hash);
        }
    }

    private async createTemplate(postgresVersion: string, hash: DatabaseTemplateHash): Promise<FreshDatabaseResult> {
        if (this.createTemplateLock) {
            await this.asyncTrigger.waitForChange();
            return await this.getFreshDatabase(postgresVersion, hash);
        } else {
            this.createTemplateLock = true;
            try {
                const masterDatabase = await this.postgresClusterCollection.getPostgresUrl(postgresVersion);
                const newDbName = await tmpDatabaseName();
                await performanceMeasure(`createBlankDatabase "${newDbName}"`, async () => {
                    await withConnectPg(masterDatabase, async (client) => {
                        await createBlankDatabase(client, newDbName);
                    });
                });
                const commitToken = await newCommitToken();
                const registeredAt = new Date();
                this.templates.set(postgresVersion, hash, {
                    type: "WaitingForCommit",
                    registeredAt: registeredAt
                });
                this.asyncTrigger.triggerChange();
                this.commitTokens.set(commitToken, {
                    postgresVersion: postgresVersion,
                    hash: hash,
                    masterUrl: masterDatabase,
                    registeredAt: registeredAt,
                    templateDb: newDbName
                });
                return {
                    type: "CreateTemplate",
                    commitToken: commitToken,
                    templateUrl: urlReplaceDbName(masterDatabase, newDbName)
                };
            } finally {
                this.createTemplateLock = false;
                this.asyncTrigger.triggerChange();
            }
        }
    }

    /**
     * Call this if `getFreshDatabase` returns `CreateTemplate`.
     *
     * Note: You must be completely disconnected from the database you were
     * given
     *
     * @returns The new database url that you should use. Returns `null` if the
     * `commitToken` is invalid
     */
    async commitTemplate(commitToken: string): Promise<PostgresUrl | null> {
        const data = this.commitTokens.get(commitToken);
        if (data === undefined) {
            return null;
        }

        this.commitTokens.delete(commitToken);

        const newDbName = await tmpDatabaseName();

        await performanceMeasure(`cloneDatabase "${data.templateDb}" -> "${newDbName}"`, async () => {
            await withConnectPg(data.masterUrl, async (client) => {
                await cloneDatabase(client, data.templateDb, newDbName);
            });
        });

        const check = this.templates.get(data.postgresVersion, data.hash);
        if (check === undefined || check.type === "WaitingForCommit") {
            this.templates.set(data.postgresVersion, data.hash, {
                type: "TemplateReady",
                masterUrl: data.masterUrl,
                readyClones: [],
                templateDb: data.templateDb
            });
        }

        this.asyncTrigger.triggerChange();

        return urlReplaceDbName(data.masterUrl, newDbName);
    }

    /**
     * Delete a database that you are finished with, to free up disk space.
     *
     * Note: You must be disconnected from the database
     */
    async dropDatabase(url: PostgresUrl): Promise<void> {
        const p = parse(url);
        const database = p.database;
        if (database === null || database === undefined) {
            return;
        }

        if (p.port !== null && p.port !== undefined) {
            const cluster = await getDatabaseClusterOnPort(this.postgresClusterCollection, p.port);
            if (cluster !== null) {
                await performanceMeasure(`drop database "${database}"`, async () => {
                    await withConnectPg(cluster.url, async (client) => {
                        await dropDatabase(client, database);
                    });
                });
            }
        }
    }

    /**
     * Return the database back into the pool, after you are finished using
     * it.
     *
     * The database must be in the EXACT same state as you got it. The best
     * way to ensure this is to BEGIN a transaction and then ROLLBACK at the
     * end (or simply disconnect). You may also want to snapshot the state of
     * all sequences at the beginning and restore them at the end (since
     * sequences are not rolled back via transaction mechanics)
     *
     * Note: You must be disconnected from the database before you call this
     */
    async releaseDatabase(url: PostgresUrl, hash: DatabaseTemplateHash): Promise<void> {
        const p = parse(url);
        if (p.database === null || p.database === undefined) {
            return;
        }

        if (p.port !== null && p.port !== undefined) {
            const cluster = await getDatabaseClusterOnPort(this.postgresClusterCollection, p.port);
            if (cluster !== null) {
                const instance = this.templates.get(cluster.postgresVersion, hash);
                if (instance !== undefined) {
                    if (instance.type === "TemplateReady") {
                        instance.readyClones.push(p.database);
                        this.asyncTrigger.triggerChange();
                        return;
                    }
                }
            }
        }

        // If we get to this point, then it means that there was a problem
        // releasing the database back into the pool, so we just drop it
        await this.dropDatabase(url);
    }

    async close(): Promise<void> {
        await performanceMeasure("Canceling templateClonerWorker", async () => {
            this.templateClonerWorkerRunningCancellationToken.cancel = true;
            this.asyncTrigger.triggerChange();
            while (this.templateClonerWorkerRunning) {
                await delay(20);
            }
        });

        await performanceMeasure("Closing", async () => {
            await this.postgresClusterCollection.close();
        });
    }

    private commitTokens = new Map<CommitToken, CommitTokenData>();
    private templates = new DatabaseTemplateMap<DatabaseTemplateState>();

    private templateClonerWorkerRunningCancellationToken = { cancel: false };
    private templateClonerWorkerRunning = true;

    private createTemplateLock = false;

    private asyncTrigger = new AsyncTrigger();

    private postgresClusterCollection = new PostgresClusterCollection();
}

export type FreshDatabaseResult = FreshDatabaseResult.Ready | FreshDatabaseResult.CreateTemplate;

export type CommitToken = string;

namespace FreshDatabaseResult {
    export interface Ready {
        type: "Ready";
        url: PostgresUrl;
    }

    export interface CreateTemplate {
        type: "CreateTemplate";
        templateUrl: PostgresUrl;

        /**
         * Can only be used once
         */
        commitToken: CommitToken;
    }
}

export interface ClusterStatus {
    postgresVersion: string;
    masterUrl: string;
    databases: {
        hash: string;
        pending: {
            registeredAt: Date;
            commitToken: string;
            templateDb: string;
        }[] | null;
        templateDb: string | null;
        readyClones: string[];
    }[];
}

type DatabaseName = string;

type DatabaseTemplateState = DatabaseTemplateState.WaitingForCommit | DatabaseTemplateState.TemplateReady;

interface CommitTokenData {
    postgresVersion: string;
    hash: DatabaseTemplateHash;
    masterUrl: PostgresUrl;
    registeredAt: Date;
    templateDb: DatabaseName;
}

namespace DatabaseTemplateState {
    export interface WaitingForCommit {
        type: "WaitingForCommit";
        registeredAt: Date;
    }

    export interface TemplateReady {
        type: "TemplateReady";
        masterUrl: PostgresUrl;
        templateDb: DatabaseName;
        readyClones: DatabaseName[];
    }
}

async function createBlankDatabase(client: pg.Client, dbName: string): Promise<void> {
    await client.query(`CREATE DATABASE ${dbName} WITH TEMPLATE template0 ENCODING='UTF8'`);
}

async function cloneDatabase(client: pg.Client, source: string, newName: string): Promise<void> {
    await client.query(`CREATE DATABASE ${newName} WITH TEMPLATE ${source}`);
}

async function dropDatabase(client: pg.Client, dbName: string): Promise<void> {
    await client.query(`DROP DATABASE ${dbName}`);
}

/**
 * Connect to the same database cluster, but a different database
 */
export function urlReplaceDbName(url: PostgresUrl, dbName: string): PostgresUrl {
    const p = parse(url);
    return `postgres://${p.user}:${p.password}@${p.host}:${p.port}/${dbName}${p.ssl === true ? "?ssl=true" : ""}`;
}

async function templateClonerWorker(cancellationToken: { cancel: boolean }, templates: DatabaseTemplateMap<DatabaseTemplateState>, asyncTrigger: AsyncTrigger): Promise<void> {
    while (!cancellationToken.cancel) {
        const neediest = getNeediestTemplate(templates);
        if (neediest !== null) {
            const newDbName = await tmpDatabaseName();
            await performanceMeasure(`worker cloneDatabase "${neediest.templateDb}" -> "${newDbName}"`, async () => {
                try {
                    await withConnectPg(neediest.masterUrl, async (client) => {
                        await cloneDatabase(client, neediest.templateDb, newDbName);
                        neediest.readyClones.push(newDbName);
                        asyncTrigger.triggerChange();
                    });
                } catch (err) {
                    console.warn("worker error cloning database:\n\n" + err.message + "\n" + err.stack);
                    await delay(5000);
                }
            });
        } else {
            await asyncTrigger.waitForChange();
        }
    }
}

/**
 * Finds the database template that has the least amount of `readyClones`
 */
function getNeediestTemplate(templates: DatabaseTemplateMap<DatabaseTemplateState>): DatabaseTemplateState.TemplateReady | null {
    let result: DatabaseTemplateState.TemplateReady | null = null;
    for (const state of templates.values()) {
        if (state.type === "TemplateReady") {
            if (state.readyClones.length < MAX_READY_CLONES) {
                if (result === null || state.readyClones.length < result.readyClones.length) {
                    result = state;
                }
            }
        }
    }
    return result;
}

async function getDatabaseClusterOnPort(postgresClusterCollection: PostgresClusterCollection, port: string): Promise<PostgresCluster | null> {
    for (const cluster of await postgresClusterCollection.getPostgresClusterUrls()) {
        if (parse(cluster.url).port === port) {
            return cluster;
        }
    }

    return null;
}

async function newCommitToken(): Promise<string> {
    return await new Promise<string>((resolve, reject) => {
        crypto.randomBytes(16, (err, buf) => {
            if (err as any as boolean) {
                reject(err);
                return;
            }

            const commitToken = buf.toString("hex");
            resolve(commitToken);
        });
    });
}

async function tmpDatabaseName(): Promise<string> {
    return await new Promise<string>((resolve, reject) => {
        crypto.randomBytes(16, (err, buf) => {
            if (err as any as boolean) {
                reject(err);
                return;
            }

            const dbName = "tmp_" + buf.toString("hex");
            resolve(dbName);
        });
    });
}

async function delay(millis: number): Promise<void> {
    return await new Promise<void>(resolve => setTimeout(resolve, millis));
}

// --------------------------------------------------------------------------
// Test code
// --------------------------------------------------------------------------

async function test(): Promise<void> {
    console.log("start");

    const service = new TemporaryDatabaseService();
    try {
        console.log(await service.getStatus());
        const result1 = await service.getFreshDatabase("10.10", "hash-1");
        console.log(await service.getStatus());
        console.log("result1", result1);
        if (result1.type === "CreateTemplate") {
            await delay(1000);
            console.log("commit...");
            await service.commitTemplate(result1.commitToken);
            console.log("commit done");
        }

        await delay(10000);

        console.log("result2...");
        const result2 = await service.getFreshDatabase("10.10", "hash-1");
        console.log("result2:", result2);

    } finally {
        await service.close();
    }
}

if (require.main === module) {
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    test();
}
