import * as pg from "pg";

export async function connectPg(url: string): Promise<pg.Client> {
    const client = new pg.Client(url);
    return await new Promise<pg.Client>((resolve, reject) => {
        client.connect(err => {
            if (err as unknown as boolean) {
                reject(err);
                return;
            }
            resolve(client);
        });
    });
}

export async function closePg(conn: pg.Client): Promise<void> {
    return await new Promise<void>((resolve, reject) => {
        conn.end(err => {
            if (err as unknown as boolean) {
                reject(err);
                return;
            }
            resolve();
        });
    });
}

export async function withConnectPg<A>(databaseUrl: string, action: (client: pg.Client) => Promise<A>): Promise<A> {
    const client = await connectPg(databaseUrl);
    try {
        return await action(client);
    } finally {
        await closePg(client);
    }
}
