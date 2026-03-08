const REDACTION_PATTERNS: Array<[RegExp, string]> = [
  [/\b\d{10}\b/g, "[NSS_REDACTED]"],
  [/\b[A-Z]{4}\d{6}[A-Z0-9]{8}\b/gi, "[CURP_REDACTED]"],
  [/\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b/g, "[EMAIL_REDACTED]"],
  [/\b(?:\+?52\s?)?(?:\d[\s-]?){10}\b/g, "[PHONE_REDACTED]"]
];

export function redactText(input: string): string {
  return REDACTION_PATTERNS.reduce(
    (acc, [pattern, replacement]) => acc.replace(pattern, replacement),
    input
  );
}

export function redactUnknown<T>(value: T): T {
  if (typeof value === "string") {
    return redactText(value) as T;
  }
  if (Array.isArray(value)) {
    return value.map((item) => redactUnknown(item)) as T;
  }
  if (value && typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const [key, innerValue] of Object.entries(value as Record<string, unknown>)) {
      output[key] = redactUnknown(innerValue);
    }
    return output as T;
  }
  return value;
}
