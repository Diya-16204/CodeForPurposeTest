/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import multer from "multer";
import { env } from "./env.js";

const allowedExtensions = new Set([".csv", ".json", ".xlsx", ".sqlite", ".db"]);

fs.mkdirSync(env.uploadDir, { recursive: true });

const storage = multer.diskStorage({
  destination: (request, file, callback) => {
    callback(null, env.uploadDir);
  },
  filename: (request, file, callback) => {
    const extension = path.extname(file.originalname || "").toLowerCase();
    callback(null, `${crypto.randomUUID()}${extension}`);
  }
});

const fileFilter = (request, file, callback) => {
  const extension = path.extname(file.originalname || "").toLowerCase();

  if (!allowedExtensions.has(extension)) {
    const error = new Error("Unsupported dataset type. Upload CSV, JSON, Excel, SQLite, or DB files only.");
    error.status = 400;
    callback(error);
    return;
  }

  callback(null, true);
};

export const uploadDataset = multer({
  fileFilter,
  limits: {
    fileSize: env.maxUploadMb * 1024 * 1024,
    files: 1
  },
  storage
}).single("dataset");

export const uploadDatasets = multer({
  fileFilter,
  limits: {
    fileSize: env.maxUploadMb * 1024 * 1024,
    files: 8
  },
  storage
}).array("datasets", 8);
