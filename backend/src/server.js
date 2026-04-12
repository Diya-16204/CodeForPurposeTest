/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import cors from "cors";
import express from "express";
import helmet from "helmet";
import morgan from "morgan";
import { env, isAllowedOrigin } from "./config/env.js";
import { connectMongo } from "./db/mongoose.js";
import { errorHandler, notFoundHandler } from "./middleware/errorHandler.js";
import { chatRoutes } from "./routes/chatRoutes.js";
import { uploadRoutes } from "./routes/uploadRoutes.js";

const app = express();

app.use(helmet());
app.use(cors({
  origin: (origin, callback) => {
    if (isAllowedOrigin(origin, env.corsOrigins)) {
      callback(null, true);
      return;
    }

    callback(new Error("Origin not allowed by CORS."));
  }
}));
app.use(express.json({ limit: "1mb" }));
app.use(morgan(env.nodeEnv === "production" ? "combined" : "dev"));

app.get("/health", (request, response) => {
  response.json({ service: "talk-to-data-backend", status: "ok" });
});

app.use("/api", uploadRoutes);
app.use("/api", chatRoutes);
app.use(notFoundHandler);
app.use(errorHandler);

await connectMongo();

app.listen(env.port, () => {
  console.log(`Talk to Data backend listening on port ${env.port}`);
});
