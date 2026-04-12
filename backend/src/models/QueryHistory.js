/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import mongoose from "mongoose";

const queryHistorySchema = new mongoose.Schema(
  {
    analyticsSidebar: {
      chart_type: { default: "table", type: String },
      data_points: { default: [], type: [mongoose.Schema.Types.Mixed] },
      outliers_noted: { default: [], type: [String] }
    },
    datasetId: { index: true, required: true, type: String },
    insightNarrative: { required: true, type: String },
    prompt: { required: true, trim: true, type: String },
    transparency: {
      data_sources: { default: [], type: [String] },
      metric_definition_used: { default: "Dynamic schema extraction", type: String }
    }
  },
  { timestamps: true }
);

queryHistorySchema.index({ datasetId: 1, createdAt: -1 });

export const QueryHistory = mongoose.model("QueryHistory", queryHistorySchema);
