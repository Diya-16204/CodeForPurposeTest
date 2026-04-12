/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import express from "express";
import { DatasetMetadata } from "../models/DatasetMetadata.js";
import { MetricDefinition } from "../models/MetricDefinition.js";
import { QueryHistory } from "../models/QueryHistory.js";
import { askDataset } from "../services/aiEngineClient.js";
import { scrubPII } from "../utils/piiGuard.js";

export const chatRoutes = express.Router();

const handleChat = async (request, response, next) => {
  try {
    const { dataset_id: datasetId, prompt } = request.body || {};

    if (!datasetId || !prompt || typeof prompt !== "string") {
      const error = new Error("Send dataset_id and a question to continue.");
      error.status = 400;
      throw error;
    }

    const dataset = await DatasetMetadata.findOne({ datasetId }).lean();
    if (!dataset) {
      const error = new Error("Upload and process this dataset before asking questions.");
      error.status = 404;
      throw error;
    }

    const metricDefinitions = await MetricDefinition.find({ datasetId })
      .select("-_id metricName formula sourceColumns description")
      .lean();

    const aiResponse = await askDataset({
      datasetId: dataset.aiEngineDatasetId,
      metricDefinitions,
      prompt,
      schema: {
        columns: dataset.columns,
        tables: dataset.tables
      },
      sourceLabel: dataset.sourceLabel
    });

    const safeResponse = scrubPII(aiResponse);

    await QueryHistory.create({
      analyticsSidebar: safeResponse.analytics_sidebar,
      datasetId,
      insightNarrative: safeResponse.insight_narrative,
      prompt: scrubPII(prompt),
      transparency: safeResponse.transparency
    });

    await DatasetMetadata.updateOne({ datasetId }, { $set: { updatedAt: new Date() } });

    response.json(safeResponse);
  } catch (error) {
    next(error);
  }
};

chatRoutes.post("/chat", handleChat);
chatRoutes.post("/ask-query", handleChat);
