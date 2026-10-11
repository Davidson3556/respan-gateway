import { gatewayFetch } from "@/lib/api";
import { ANY, type RequestFilter } from "@/lib/requests";

export type ExportFormat = "jsonl" | "csv";

/** The export URL for the filters in force; unset filters and the free-text search are left out. */
export function exportPath(format: ExportFormat, filter: Partial<RequestFilter>, since?: string): string {
  const query = new URLSearchParams({ format });
  for (const name of ["kind", "status", "provider", "model", "client"] as const) {
    const value = filter[name];
    if (value && value !== ANY) query.set(name, value);
  }
  if (since) query.set("since", since);
  return `/ui/api/requests/export?${query}`;
}

/** Fetches the export with the gateway key, then saves it as a file (a plain link couldn't send the key). */
export async function downloadExport(format: ExportFormat, filter: Partial<RequestFilter>): Promise<void> {
  const res = await gatewayFetch(exportPath(format, filter));
  const url = URL.createObjectURL(await res.blob());
  try {
    const link = document.createElement("a");
    link.href = url;
    link.download = `requests.${format}`;
    document.body.append(link);
    link.click();
    link.remove();
  } finally {
    URL.revokeObjectURL(url);
  }
}
