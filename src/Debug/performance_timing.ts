export async function performanceMeasure<A>(description: string, action: () => Promise<A>): Promise<A> {
    console.log(`${description}...`);
    const before = new Date().getTime();
    const result = await action();
    const after = new Date().getTime();
    console.log(formatMilliseconds(after - before), description);
    return result;
}

function formatMilliseconds(millis: number): string {
    const secs = Math.floor(millis / 1000);
    const frac = millis % 1000;
    const fracStr = `${frac}`;
    return `${secs}.${fracStr.padStart(3, "0")}s`;
}
