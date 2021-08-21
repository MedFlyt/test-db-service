import * as express from "express";

type Handler = (query: { [index: string]: string | undefined }) => Promise<HttpResponse>;

export interface HttpResponse {
    status: number;
    body: any;
}

export function GET(serverName: string, express: express.Express, path: string, handler: Handler): void {
    express.get(path, makeExpressHandler(serverName, handler));
}

export function POST(serverName: string, express: express.Express, path: string, handler: Handler): void {
    express.post(path, makeExpressHandler(serverName, handler));
}

function makeExpressHandler(serverName: string, handler: Handler): (req: express.Request, rsp: express.Response) => void {
    return (req: express.Request, rsp: express.Response): void => {
        rsp.setHeader("Server", serverName);
        rsp.setHeader("Connection", "close");
        handler(req.query as any).then(
            (response) => {
                rsp.contentType("application/json").status(response.status).send(JSON.stringify(response.body, null, 4) + "\n");
            },
            (err) => {
                rsp.contentType("text/plain").status(500).send("Internal Server Error\n\n" + err.message + "\n" + err.stack + "\n");
            });
    };
}
