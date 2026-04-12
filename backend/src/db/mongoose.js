/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import mongoose from "mongoose";
import { env } from "../config/env.js";

export const connectMongo = async () => {
  mongoose.set("strictQuery", true);

  await mongoose.connect(env.mongoUri, {
    autoIndex: env.nodeEnv !== "production"
  });

  return mongoose.connection;
};
