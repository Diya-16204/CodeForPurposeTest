/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import mongoose from "mongoose";

const metricDefinitionSchema = new mongoose.Schema(
  {
    datasetId: { index: true, required: true, type: String },
    description: { default: "Dynamic metric inferred from uploaded schema", trim: true, type: String },
    formula: { required: true, trim: true, type: String },
    metricName: { index: true, required: true, trim: true, type: String },
    sourceColumns: { default: [], type: [String] }
  },
  { timestamps: true }
);

metricDefinitionSchema.index({ datasetId: 1, metricName: 1 }, { unique: true });

export const MetricDefinition = mongoose.model("MetricDefinition", metricDefinitionSchema);
