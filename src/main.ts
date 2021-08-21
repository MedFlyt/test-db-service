import { assertNever } from "assert-never";
import * as childProcess from "child_process";
import * as path from "path";

import { PORT, startHttpServer } from "./app_http_server";
import { backgroundServerSendShutdown, getExistingServerStatus } from "./background_server_status";

async function help(): Promise<never> {
    console.log(`Usage: ${process.argv[1]} [start|status|stop]`);
    console.log();
    console.log("  --version         Print version and exit");
    console.log("  --help            Print this help info and exit");
    console.log();
    console.log("  start (default)   Start the server in the background");
    console.log("  start-foreground  Start the server in the foreground");
    console.log("  status            Print the status of the running server and exit");
    console.log("  stop              Exit a server running in the background");
    return process.exit(0);
}

export async function main(): Promise<never> {
    const args = process.argv.slice(2);

    if (args.indexOf("--help") >= 0) {
        return await help();
    } else if (args.indexOf("--version") >= 0) {
        return await version();
    } else if (args.length === 0) {
        // Default command
        return await startBackground(new Date());
    } else if (args.length !== 1) {
        return await invalid(args);
    } else if (args[0] === "start") {
        return await startBackground(new Date());
    } else if (args[0] === "start-foreground") {
        return await startForeground();
    } else if (args[0] === "status") {
        return await status();
    } else if (args[0] === "stop") {
        return await stop();
    } else {
        return await invalid(args);
    }
}

function getVersion(): string {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require("../package.json").version;
}

async function version(): Promise<never> {
    console.log(getVersion());
    return process.exit(0);
}

async function startBackground(startTime: Date): Promise<never> {
    const status = await getExistingServerStatus();
    switch (status.type) {
        case "Running":
            console.log("Server already running");
            return process.exit(0);
        case "NotRunning":
            return await launchBackground();
        case "Faulty":
            if (new Date().getTime() - startTime.getTime() > 30000) {
                console.error("Error starting server");
                console.error("Existing background server is FAULTY:");
                console.error("  " + status.errMessage);
                return process.exit(2);
            } else {
                await delay(100);
                return await startBackground(startTime);
            }
        default:
            return assertNever(status);
    }
}

async function launchBackground(): Promise<never> {
    // Reference:
    // <https://github.com/silverwind/daemonize-process/blob/72815a7291dc4e115f0df11107c559e552fe626f/index.js>

    const id = "_DAEMONIZE_PROCESS";

    console.log("Starting server in background...");

    const script = process.argv[1];
    const child = childProcess.spawn(process.execPath, [script].concat(["start-foreground"]), {
        env: {
            ...process.env,
            [id]: "1"
        },
        cwd: path.parse(process.cwd()).root,
        stdio: "ignore",
        detached: true
    });
    child.unref();

    // Wait for background process to start:

    const startTime = new Date();
    while (true) {
        const status = await getExistingServerStatus();
        if (status.type === "Running") {
            console.log("Success");
            console.log(`Server is listening on: http://localhost:${PORT}`);
            return process.exit(0);
        }
        if (child.exitCode !== null) {
            console.error(`Error starting background server: exit-code: ${child.exitCode}`);
            console.error("Try running `start-foreground` to diagnose");
            return process.exit(1);
        }
        if (new Date().getTime() - startTime.getTime() > 30000) {
            switch (status.type) {
                case "NotRunning":
                    console.error("Error starting background server: NotRunning after timeout");
                    console.error("WARNING: Background process may still be alive");
                    return process.exit(1);
                case "Faulty":
                    console.error("Error starting background server: HTTP server is FAULTY after timeout:");
                    console.error("  " + status.errMessage);
                    return process.exit(2);
                default:
                    return assertNever(status);
            }
        }
        await delay(50);
    }
}

async function startForeground(): Promise<never> {
    try {
        await startHttpServer(`test-db-service ${getVersion()}`);
        return process.exit(0);
    } catch (err) {
        console.error("Error starting server:");
        console.error(`  ${err.code} ${err.message}`);
        return process.exit(1);
    }
}

async function status(): Promise<never> {
    const status = await getExistingServerStatus();
    switch (status.type) {
        case "Running":
            if (getVersion() === status.version) {
                console.log("Server is running");
                return process.exit(0);
            } else {
                console.log("Server is running");
                console.warn(`WARNING Running server has different version: ${status.version}`);
                return process.exit(0);
            }
        case "NotRunning":
            console.log("Server is NOT running");
            return process.exit(1);
        case "Faulty":
            console.log("Server is FAULTY:");
            console.log("  " + status.errMessage);
            return process.exit(2);
        default:
            return assertNever(status);
    }
}

async function stop(): Promise<never> {
    await backgroundServerSendShutdown();

    const startTime = new Date();
    while (true) {
        const status = await getExistingServerStatus();
        if (status.type === "NotRunning") {
            return process.exit(0);
        }
        if (new Date().getTime() - startTime.getTime() > 30000) {
            switch (status.type) {
                case "Running":
                    console.error("Error stopping server");
                    console.error("Server is still running");
                    return process.exit(1);
                case "Faulty":
                    console.error("Error stopping server");
                    console.error("Server is FAULTY:");
                    console.error("  " + status.errMessage);
                    return process.exit(2);
                default:
                    return assertNever(status);
            }
        }
        await delay(50);
    }
}

async function invalid(args: string[]): Promise<never> {
    console.log(`Invalid args: ${args.join(" ")}`);
    console.log();
    console.log("Run:");
    console.log();
    console.log(`  ${process.argv[1]} --help`);
    console.log();
    console.log("For usage and help");
    return process.exit(1);
}

async function delay(millis: number): Promise<void> {
    return await new Promise<void>(resolve => setTimeout(resolve, millis));
}

if (require.main === module) {
    require("source-map-support/register");
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    main();
}
