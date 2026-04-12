/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import mongoose from "mongoose";

const columnSchema = new mongoose.Schema(
  {
    isSensitive: { default: false, type: Boolean },
    name: { required: true, trim: true, type: String },
    nullable: { default: true, type: Boolean },
    type: { required: true, trim: true, type: String }
  },
  { _id: false }
);

const tableSchema = new mongoose.Schema(
  {
    columns: { default: [], type: [columnSchema] },
    name: { required: true, trim: true, type: String },
    rowCount: { default: 0, type: Number }
  },
  { _id: false }
);

const datasetMetadataSchema = new mongoose.Schema(
  {
    aiEngineDatasetId: { index: true, required: true, type: String },
    columns: { default: [], type: [columnSchema] },
    dashboardPreview: { default: {}, type: mongoose.Schema.Types.Mixed },
    datasetId: { index: true, required: true, type: String, unique: true },
    fileType: { required: true, trim: true, type: String },
    originalFilename: { required: true, trim: true, type: String },
    piiColumns: { default: [], type: [String] },
    relationships: { default: [], type: [mongoose.Schema.Types.Mixed] },
    rowCount: { default: 0, type: Number },
    sourceLabel: { required: true, trim: true, type: String },
    sourceFiles: { default: [], type: [mongoose.Schema.Types.Mixed] },
    storageMode: { required: true, trim: true, type: String },
    tables: { default: [], type: [tableSchema] },
    uploadInsight: { default: "", trim: true, type: String }
  },
  { timestamps: true }
);

export const DatasetMetadata = mongoose.model("DatasetMetadata", datasetMetadataSchema);
