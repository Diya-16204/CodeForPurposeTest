/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
const emailPattern = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi;
const phonePattern = /\b(?:(?:phone|mobile|tel)[:\s]*|\+)\d[\d ().-]{7,}\d\b/gi;
const longNumberPattern = /\b\d{8,}\b/g;

export const scrubText = (value) => value
  .replace(emailPattern, "[redacted email]")
  .replace(phonePattern, "[redacted number]")
  .replace(longNumberPattern, "[redacted number]");

export const scrubPII = (value) => {
  if (typeof value === "string") {
    return scrubText(value);
  }

  if (Array.isArray(value)) {
    return value.map((item) => scrubPII(item));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, scrubPII(item)])
    );
  }

  return value;
};
