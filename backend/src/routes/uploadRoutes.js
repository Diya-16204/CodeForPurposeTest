/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import fs from "node:fs/promises";
import express from "express";
import { uploadDataset, uploadDatasets } from "../config/multer.js";
import { DatasetMetadata } from "../models/DatasetMetadata.js";
import { MetricDefinition } from "../models/MetricDefinition.js";
import { QueryHistory } from "../models/QueryHistory.js";
import { ingestDataset, ingestDatasets } from "../services/aiEngineClient.js";
import { scrubPII } from "../utils/piiGuard.js";

export const uploadRoutes = express.Router();
const numericTypePattern = /(int|float|double|decimal|number|numeric)/i;

const persistWorkspace = async (ingestion, fallbackFilename) => {
  const safeIngestion = scrubPII(ingestion);

  const record = await DatasetMetadata.create({
    aiEngineDatasetId: safeIngestion.dataset_id,
    columns: safeIngestion.schema?.columns || [],
    dashboardPreview: safeIngestion.dashboard_preview || {},
    datasetId: safeIngestion.dataset_id,
    fileType: safeIngestion.file_type,
    originalFilename: fallbackFilename,
    piiColumns: safeIngestion.pii_columns || [],
    relationships: safeIngestion.relationships || [],
    rowCount: safeIngestion.row_count || 0,
    sourceFiles: safeIngestion.source_files || [],
    sourceLabel: fallbackFilename,
    storageMode: safeIngestion.storage_mode,
    tables: safeIngestion.schema?.tables || [],
    uploadInsight: safeIngestion.upload_insight || ""
  });

  const metricWrites = (safeIngestion.schema?.columns || [])
    .filter((column) => numericTypePattern.test(column.type) && !column.isSensitive)
    .map((column) => ({
      updateOne: {
        filter: { datasetId: record.datasetId, metricName: column.name },
        update: {
          $set: {
            description: `Aggregated ${column.name} from the uploaded dataset workspace`,
            formula: `sum(${column.name})`,
            sourceColumns: [column.name]
          }
        },
        upsert: true
      }
    }));

  if (metricWrites.length > 0) {
    await MetricDefinition.bulkWrite(metricWrites, { ordered: false });
  }

  return {
    dashboard_preview: record.dashboardPreview,
    dataset_id: record.datasetId,
    file_type: record.fileType,
    original_filename: record.originalFilename,
    pii_columns: record.piiColumns,
    relationships: record.relationships,
    row_count: record.rowCount,
    schema: {
      columns: record.columns,
      tables: record.tables
    },
    source_files: record.sourceFiles,
    storage_mode: record.storageMode,
    upload_insight: record.uploadInsight
  };
};

uploadRoutes.post("/upload", (request, response, next) => {
  uploadDataset(request, response, async (uploadError) => {
    if (uploadError) {
      next(uploadError);
      return;
    }

    if (!request.file) {
      const error = new Error("Choose a dataset file before uploading.");
      error.status = 400;
      next(error);
      return;
    }

    try {
      const ingestion = await ingestDataset(request.file);
      const payload = await persistWorkspace(ingestion, `Uploaded File: ${request.file.originalname}`);
      response.status(201).json(payload);
    } catch (error) {
      next(error);
    } finally {
      await fs.rm(request.file.path, { force: true });
    }
  });
});

uploadRoutes.post("/upload-multiple", (request, response, next) => {
  uploadDatasets(request, response, async (uploadError) => {
    if (uploadError) {
      next(uploadError);
      return;
    }

    const files = request.files || [];
    if (!Array.isArray(files) || files.length === 0) {
      const error = new Error("Choose at least one dataset file before uploading.");
      error.status = 400;
      next(error);
      return;
    }

    try {
      const ingestion = await ingestDatasets(files);
      const fallbackFilename = `Connected Workspace: ${files.map((file) => file.originalname).join(", ")}`;
      const payload = await persistWorkspace(ingestion, fallbackFilename);
      response.status(201).json(payload);
    } catch (error) {
      next(error);
    } finally {
      await Promise.all(files.map((file) => fs.rm(file.path, { force: true })));
    }
  });
});

uploadRoutes.post("/analyze", async (request, response, next) => {
  try {
    const { dataset_id: datasetId } = request.body || {};
    if (!datasetId) {
      const error = new Error("Send dataset_id to fetch combined analytics.");
      error.status = 400;
      throw error;
    }

    const dataset = await DatasetMetadata.findOne({ datasetId }).lean();
    if (!dataset) {
      const error = new Error("Upload and process datasets before analyzing them.");
      error.status = 404;
      throw error;
    }

    response.json({
      dashboard_preview: dataset.dashboardPreview || {},
      dataset_id: dataset.datasetId,
      relationships: dataset.relationships || [],
      source_files: dataset.sourceFiles || [],
      upload_insight: dataset.uploadInsight || ""
    });
  } catch (error) {
    next(error);
  }
});

uploadRoutes.get("/datasets/:datasetId/history", async (request, response, next) => {
  try {
    const limit = Math.min(Number(request.query.limit || 10), 10);
    const history = await QueryHistory.find({ datasetId: request.params.datasetId })
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean();

    response.json({
      dataset_id: request.params.datasetId,
      searches: history.map((item) => ({
        analytics_sidebar: item.analyticsSidebar,
        created_at: item.createdAt,
        insight_narrative: item.insightNarrative,
        prompt: item.prompt,
        transparency: item.transparency
      }))
    });
  } catch (error) {
    next(error);
  }
});
