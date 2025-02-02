import { assertNever } from "assert-never";

import { PostgresServer } from "../launch_postgres";

/**
 * Creates and launches a new blank PostgreSQL database cluster.
 *
 * Use `getPostgresUrl` to get the url of the new cluster and then you should
 * create your own new database inside the cluster (using CREATE DATABASE)
 */
export class PostgresClusterInstance {
    /**
     * @param postgresVersion The version number of PostgreSQL that should be
     * used. Example: `10.10`
     */
    constructor(postgresVersion: string) {
        this.postgresVersion = postgresVersion;
        this.status = { type: "Starting" };

        // eslint-disable-next-line @typescript-eslint/no-floating-promises
        this.launch();
    }

    /**
     * Get the connection url for the "master" database (usually called "postgres")
     */
    async getPostgresUrl(): Promise<string> {
        switch (this.status.type) {
            case "Starting":
                return await new Promise<string>((resolve) => {
                    this.serverReadyCallbacks.push((server) => {
                        resolve(server.url);
                    });
                });
            case "Running":
                return this.status.server.url;
            default:
                return assertNever(this.status);
        }
    }

    /**
     * Shutdown and delete the cluster
     */
    async close(): Promise<void> {
        switch (this.status.type) {
            case "Running": {
                await this.status.server.close();
                break;
            }
            case "Starting": {
                const server = await new Promise<PostgresServer>((resolve) => {
                    this.serverReadyCallbacks.push((server) => {
                        resolve(server);
                    });
                });
                await server.close();
                break;
            }
            default:
                return assertNever(this.status);
        }
    }

    private readonly postgresVersion: string;
    private status: Status;

    private serverReadyCallbacks: ((server: PostgresServer) => void)[] = [];

    private async launch(): Promise<void> {
        const server = await PostgresServer.start(this.postgresVersion);
        this.status = {
            type: "Running",
            server: server
        };
        for (const serverReadyCallback of this.serverReadyCallbacks) {
            serverReadyCallback(server);
        }

        // Free memory:
        this.serverReadyCallbacks = [];
    }
}

type Status = Status.Starting | Status.Running;

declare namespace Status {
    export interface Starting {
        type: "Starting";
    }

    export interface Running {
        type: "Running";
        server: PostgresServer;
    }
}


// --------------------------------------------------------------------------
// Test code
// --------------------------------------------------------------------------

async function test(): Promise<void> {
    const postgresVersion = "10.10";

    console.log("start");
    const instance = new PostgresClusterInstance(postgresVersion);

    console.log("get url");
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    instance.getPostgresUrl().then(url => console.log("url a", url));
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    instance.getPostgresUrl().then(url => console.log("url b", url));

    console.log("get url2");
    const url2 = await instance.getPostgresUrl();
    console.log(url2);

    console.log("delay 2000");
    await delay(2000);
    console.log("close");
    await instance.close();
}

async function delay(millis: number): Promise<void> {
    return await new Promise<void>((resolve) => {
        setTimeout(resolve, millis);
    });
}

if (require.main === module) {
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    test();
}
