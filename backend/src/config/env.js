/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import path from "node:path";
import process from "node:process";
import dotenv from "dotenv";

dotenv.config();

const numberFromEnv = (name, fallback) => {
  const rawValue = process.env[name];
  if (!rawValue) {
    return fallback;
  }

  const parsed = Number(rawValue);
  if (Number.isNaN(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive number.`);
  }

  return parsed;
};

const listFromEnv = (name, fallback) => {
  const rawValue = process.env[name];
  if (!rawValue) {
    return fallback;
  }

  return rawValue
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
};

const escapePattern = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const originToPattern = (origin) => {
  if (origin === "*") {
    return /^.*$/;
  }

  const parts = origin.split("*").map((part) => escapePattern(part));
  return new RegExp(`^${parts.join(".*")}$`);
};

export const isAllowedOrigin = (origin, allowedOrigins) => {
  if (!origin) {
    return true;
  }

  if (!allowedOrigins || allowedOrigins.length === 0) {
    return true;
  }

  return allowedOrigins.some((allowedOrigin) => originToPattern(allowedOrigin).test(origin));
};

export const env = Object.freeze({
  aiEngineUrl: process.env.AI_ENGINE_URL || "http://127.0.0.1:8000",
  corsOrigins: listFromEnv("CORS_ORIGIN", ["http://localhost:5173"]),
  maxUploadMb: numberFromEnv("MAX_UPLOAD_MB", 50),
  mongoUri: process.env.MONGODB_URI || "mongodb://127.0.0.1:27017/talk_to_data",
  nodeEnv: process.env.NODE_ENV || "development",
  port: numberFromEnv("PORT", 4000),
  uploadDir: path.resolve(process.cwd(), process.env.UPLOAD_DIR || "uploads/tmp")
});
