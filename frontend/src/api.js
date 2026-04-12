/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:4000/api";

const parseResponse = async (response) => {
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.insight_narrative || payload.detail || "The request could not be completed.");
  }
  return payload;
};

export const uploadDataset = async (file) => {
  const formData = new FormData();
  formData.append("dataset", file);

  const response = await fetch(`${API_BASE_URL}/upload`, {
    body: formData,
    method: "POST"
  });

  return parseResponse(response);
};

export const uploadDatasets = async (files) => {
  const formData = new FormData();
  files.forEach((file) => formData.append("datasets", file));

  const response = await fetch(`${API_BASE_URL}/upload-multiple`, {
    body: formData,
    method: "POST"
  });

  return parseResponse(response);
};

export const analyzeWorkspace = async (datasetId) => {
  const response = await fetch(`${API_BASE_URL}/analyze`, {
    body: JSON.stringify({ dataset_id: datasetId }),
    headers: { "Content-Type": "application/json" },
    method: "POST"
  });

  return parseResponse(response);
};

export const askDataset = async (datasetId, prompt) => {
  const response = await fetch(`${API_BASE_URL}/ask-query`, {
    body: JSON.stringify({ dataset_id: datasetId, prompt }),
    headers: { "Content-Type": "application/json" },
    method: "POST"
  });

  return parseResponse(response);
};

export const fetchRecentSearches = async (datasetId) => {
  const response = await fetch(`${API_BASE_URL}/datasets/${datasetId}/history?limit=10`);
  return parseResponse(response);
};
