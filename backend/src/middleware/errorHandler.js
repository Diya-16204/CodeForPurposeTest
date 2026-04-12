/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
export const notFoundHandler = (request, response, next) => {
  const error = new Error(`Route not found: ${request.method} ${request.originalUrl}`);
  error.status = 404;
  next(error);
};

export const errorHandler = (error, request, response, next) => {
  const status = error.status || (error.name === "MulterError" ? 400 : 500);
  const message = status >= 500 ? "The service could not complete the request." : error.message;

  if (status >= 500) {
    console.error(error);
  }

  response.status(status).json({
    query_status: "error",
    insight_narrative: message,
    analytics_sidebar: {
      chart_type: "table",
      data_points: [],
      outliers_noted: []
    },
    transparency: {
      data_sources: [],
      metric_definition_used: "Request validation"
    }
  });
};
