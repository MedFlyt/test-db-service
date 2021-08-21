import { assertNever } from "assert-never";
import * as express from "express";
import * as gracefulExit from "express-graceful-exit";

import { GET, HttpResponse, POST } from "./HttpUtils/HttpRouter";
import { TemporaryDatabaseService } from "./TemporaryDatabaseService/TemporaryDatabaseService";

/**
 * This port is hardcoded because this needs to be standardized so many
 * different services can use the same shared server
 */
export const PORT = 18543;

export async function startHttpServer(serverName: string): Promise<void> {
    // TODO Centralized logging (to streaming HTTP endpoint?)

    return await new Promise<void>((resolve, reject) => {
        const temporaryDatabaseService = new TemporaryDatabaseService();

        const app = express();

        GET(serverName, app, "/", async (): Promise<HttpResponse> => {
            const status = await temporaryDatabaseService.getStatus();
            return {
                status: 200,
                body: {
                    pid: process.pid,
                    status: status
                }
            };
        });

        POST(serverName, app, "/database", async (query): Promise<HttpResponse> => {
            const pg = query["pg"];
            const hash = query["hash"];
            if (pg === undefined) {
                return { status: 400, body: { error: `Required query param': "pg"` } };
            }
            if (hash === undefined) {
                return { status: 400, body: { error: `Required query param: "hash"` } };
            }
            const result = await temporaryDatabaseService.getFreshDatabase(pg, hash);
            switch (result.type) {
                case "CreateTemplate":
                    return {
                        status: 202,
                        body: { templateUrl: result.templateUrl, token: result.commitToken }
                    };
                case "Ready":
                    return {
                        status: 201,
                        body: { url: result.url }
                    };
                default:
                    return assertNever(result);
            }
        });

        POST(serverName, app, "/database/commit", async (query): Promise<HttpResponse> => {
            const token = query["token"];
            if (token === undefined) {
                return { status: 400, body: { error: `Required query param: "token"` } };
            }
            const commitResult = await temporaryDatabaseService.commitTemplate(token);
            if (commitResult === null) {
                return {
                    status: 400,
                    body: { error: "Invalid or expired commit token" }
                };
            } else {
                return {
                    status: 201,
                    body: { url: commitResult }
                };
            }
        });

        POST(serverName, app, "/database/drop", async (query): Promise<HttpResponse> => {
            const url = query["url"];
            if (url === undefined) {
                return { status: 400, body: { error: `Required query param: "url"` } };
            }
            await temporaryDatabaseService.dropDatabase(url);
            return {
                status: 204,
                body: {}
            };
        });

        POST(serverName, app, "/shutdown", async (query): Promise<HttpResponse> => {
            shutdown();
            return {
                status: 204,
                body: {}
            };
        });

        const server = app.listen(PORT, () => {
            console.log(`Server is listening on: http://localhost:${PORT}`);
        }).on("error", (err) => {
            reject(err);
        });

        gracefulExit.init(server);

        server.on("connection", (socket) => {
            socket.setNoDelay(true);
        });

        function shutdown(): void {
            gracefulExit.gracefulExitHandler(app, server, {
                exitProcess: false,
                performLastRequest: true,
                errorDuringExit: true,
                callback: () => {
                    console.log("Closing TemporaryDatabaseService");
                    // eslint-disable-next-line @typescript-eslint/no-floating-promises
                    temporaryDatabaseService.close().then(() => {
                        resolve();
                    });
                }
            });
        }

        process.on("SIGINT", () => {
            shutdown();
        });

        process.on("SIGTERM", () => {
            shutdown();
        });
    });
}
