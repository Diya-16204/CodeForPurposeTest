/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import fs from "node:fs";
import axios from "axios";
import FormData from "form-data";
import { env } from "../config/env.js";

const client = axios.create({
  baseURL: env.aiEngineUrl,
  timeout: 120000
});

export const ingestDataset = async (file) => {
  const form = new FormData();
  form.append("file", fs.createReadStream(file.path), {
    contentType: file.mimetype || "application/octet-stream",
    filename: file.originalname
  });

  const response = await client.post("/ingest", form, {
    headers: form.getHeaders(),
    maxBodyLength: Infinity,
    maxContentLength: Infinity
  });

  return response.data;
};

export const ingestDatasets = async (files) => {
  const form = new FormData();
  for (const file of files) {
    form.append("files", fs.createReadStream(file.path), {
      contentType: file.mimetype || "application/octet-stream",
      filename: file.originalname
    });
  }

  const response = await client.post("/ingest-multiple", form, {
    headers: form.getHeaders(),
    maxBodyLength: Infinity,
    maxContentLength: Infinity
  });

  return response.data;
};

export const askDataset = async ({ datasetId, metricDefinitions, prompt, schema, sourceLabel }) => {
  const response = await client.post("/chat", {
    dataset_id: datasetId,
    metric_definitions: metricDefinitions,
    prompt,
    schema,
    source_label: sourceLabel
  });

  return response.data;
};
