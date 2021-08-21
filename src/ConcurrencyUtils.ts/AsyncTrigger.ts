/**
 * Multiple listeners can wait until a change happens
 */
export class AsyncTrigger {
    triggerChange(): void {
        for (const waiter of this.waiters) {
            waiter();
        }
        this.waiters = [];
    }

    async waitForChange(): Promise<void> {
        return await new Promise<void>(resolve => {
            this.waiters.push(resolve);
        });
    }

    private waiters: (() => void)[] = [];
}
