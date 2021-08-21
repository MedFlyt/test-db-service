export type DatabaseTemplateHash = string;

/**
 * A "Map"-like class with a compound key
 */
export class DatabaseTemplateMap<T> {
    get(postgresVersion: string, hash: DatabaseTemplateHash): T | undefined {
        return this.map.get(keyToString(postgresVersion, hash));
    }

    set(postgresVersion: string, hash: DatabaseTemplateHash, value: T): void {
        this.map.set(keyToString(postgresVersion, hash), value);
    }

    public forEach(callbackfn: (value: T, postgresVersion: string, hash: DatabaseTemplateHash, map: DatabaseTemplateMap<T>) => void): void {
        this.map.forEach((value, key) => {
            const [postgresVersion, hash] = stringToKey(key);
            callbackfn(value, postgresVersion, hash, this);
        });
    }

    public values(): IterableIterator<T> {
        return this.map.values();
    }

    private map = new Map<string, T>();
}

function keyToString(postgresVersion: string, hash: DatabaseTemplateHash): string {
    return `${postgresVersion}\t${hash}`;
}

function stringToKey(key: string): [string, DatabaseTemplateHash] {
    const [postgresVersion, hash] = key.split("\t", 2);
    return [postgresVersion, hash];
}
