import XLSX from "xlsx";

export interface XlsxRow {
  [key: string]: string | number | null;
}

export function parseWorksheetRows(buffer: Buffer): XlsxRow[] {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const firstSheetName = workbook.SheetNames[0];
  if (!firstSheetName) {
    return [];
  }
  const worksheet = workbook.Sheets[firstSheetName];
  if (!worksheet) {
    return [];
  }
  return XLSX.utils.sheet_to_json<XlsxRow>(worksheet, {
    defval: null
  });
}

export function normalizeCensoRows(rows: XlsxRow[]): Array<Record<string, string>> {
  return rows.map((row) => {
    const normalized: Record<string, string> = {};
    for (const [key, value] of Object.entries(row)) {
      normalized[key.trim().toLowerCase()] = value == null ? "" : String(value).trim();
    }
    return normalized;
  });
}
