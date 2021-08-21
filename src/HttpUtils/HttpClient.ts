import * as http from "http";

export interface HttpResponse {
    statusCode: number;
    headers: { [header: string]: string | string[] | undefined };
    body: string;
}

export async function httpReq(host: string, port: number, method: string, path: string, headers: { [header: string]: string }, reqBody: string): Promise<HttpResponse> {
    const agent: http.Agent = new http.Agent({ keepAlive: false });

    return await new Promise<HttpResponse>((resolve, reject) => {
        const timeout = setTimeout(() => {
            req.destroy();
            const err = new Error("HTTP Timeout");
            (err as any).code = "ETIMEDOUT";
            reject(err);
        }, 30000);

        const req = http.request({
            agent: agent,
            host: host,
            hostname: host,
            port: port,
            method: method,
            path: path,
            headers: headers
        }, (response) => {
            let rawData = "";
            response.on("data", chunk => {
                rawData += chunk;
            });
            response.on("end", () => {
                const rsp: HttpResponse = {
                    statusCode: response.statusCode !== undefined ? response.statusCode : 0,
                    headers: response.headers,
                    body: rawData
                };
                clearTimeout(timeout);
                resolve(rsp);
            });
        });
        req.on("error", (err) => {
            clearTimeout(timeout);
            reject(err);
        });
        req.end(reqBody, "utf8");
    });
}
