import * as express from "express";

const PORT = 8585;

export function main(): void {
    console.log("Hi");

    const app = express();

    app.listen(PORT, () => {
        console.log("Listening on port " + PORT);
    });
}

main();
