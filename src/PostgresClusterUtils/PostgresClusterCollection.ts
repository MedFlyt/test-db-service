import { readPostgresVersion } from "../launch_postgres";
import { PostgresClusterInstance } from "./PostgresClusterInstance";

/**
 * Manages multiple PostgreSQL clusters (A separate cluster for each PostgreSQL
 * version)
 */
export class PostgresClusterCollection {
    constructor() {
    }

    async getPostgresUrl(postgresVersion: string): Promise<string> {
        const pgVersion = readPostgresVersion(postgresVersion);
        if (pgVersion === null) {
            throw new Error(`Invalid PostgreSQL version: "${postgresVersion}"`);
        }

        const existing = this.clusters.get(postgresVersion);
        if (existing !== undefined) {
            return await existing.getPostgresUrl();
        } else {
            const newCluster = new PostgresClusterInstance(pgVersion);
            this.clusters.set(postgresVersion, newCluster);
            return await newCluster.getPostgresUrl();
        }
    }

    async getPostgresClusterUrls(): Promise<PostgresCluster[]> {
        const result: PostgresCluster[] = [];
        for (const [postgresVersion, cluster] of this.clusters) {
            const url = await cluster.getPostgresUrl();
            result.push({
                postgresVersion: postgresVersion,
                url: url
            });
        }
        return result;
    }

    /**
     * Shutdown and delete the cluster
     */
    async close(): Promise<void> {
        const instances = [...this.clusters.values()];
        await Promise.all(instances.map(i => i.close()));
    }

    private clusters = new Map<string, PostgresClusterInstance>();
}

export interface PostgresCluster {
    postgresVersion: string;
    url: string;
}
