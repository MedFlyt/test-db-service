import { PORT } from "./app_http_server";
import { httpReq } from "./HttpUtils/HttpClient";

export type ExistingServerStatus =
    ExistingServerStatus.Running |
    ExistingServerStatus.NotRunning |
    ExistingServerStatus.Faulty;

export namespace ExistingServerStatus {
    export interface Running {
        type: "Running";
        version: string | undefined;
    }

    export interface NotRunning {
        type: "NotRunning";
    }

    export interface Faulty {
        type: "Faulty";
        errMessage: string;
        httpStatus?: number;
        errCode?: string;

    }
}

export async function getExistingServerStatus(): Promise<ExistingServerStatus> {
    try {
        const response = await httpReq("localhost", PORT, "HEAD", "/", {}, "");
        if (response.statusCode >= 200 && response.statusCode <= 299) {
            return {
                type: "Running",
                version: parseServerHeader(response.headers["server"])
            };
        } else {
            return {
                type: "Faulty",
                errMessage: `Invalid HTTP Status Code: ${response.statusCode}`,
                httpStatus: response.statusCode
            };
        }
    } catch (err) {
        if (err.code === "ECONNREFUSED") {
            return { type: "NotRunning" };
        } else if (err.code === "ECONNRESET") {
            return {
                type: "Faulty",
                errMessage: `Error connecting to server: ${err.code} ${err.message}`,
                errCode: err.code
            };
        } else if (err.code === "ETIMEDOUT") {
            return {
                type: "Faulty",
                errMessage: `Error connecting to server: ${err.code} ${err.message}`,
                errCode: err.code
            };
        } else {
            return {
                type: "Faulty",
                errMessage: `Error connecting to server: ${err.code} ${err.message}`,
                errCode: err.code
            };
        }
    }
}

function parseServerHeader(value: string | string[] | undefined): string | undefined {
    if (value === undefined) {
        return undefined;
    }

    if (typeof value === "string") {
        return value.split(" ")[1];
    }

    return undefined;
}

/**
 * Any errors are silently ignored.
 *
 * After calling this, you should call `waitTillBackgroundServerStop` to wait
 * for the server background server to complete the shut down
 */
export async function backgroundServerSendShutdown(): Promise<void> {
    try {
        await httpReq("localhost", PORT, "POST", "/shutdown", {}, "");
    } catch (err) {
        return;
    }
}

/**
 * @returns `true` if we have detected that the server is now running. Returns
 * `false` if the server is not running (after the timeout) or if there is
 * some error detecting the status
 */
export async function waitTillBackgroundServerStart(): Promise<boolean> {
    const startTime = new Date();
    while (true) {
        const status = await getExistingServerStatus();
        if (status.type === "Running") {
            return true;
        }
        if (new Date().getTime() - startTime.getTime() > 30000) {
            return false;
        }
        await delay(50);
    }
}

async function delay(millis: number): Promise<void> {
    return await new Promise<void>(resolve => setTimeout(resolve, millis));
}
