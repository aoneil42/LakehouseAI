/**
 * Export utilities for selected features — CSV, GeoJSON, GeoParquet.
 */

import type { Table } from "apache-arrow";

export type ExportFormat = "csv" | "geojson" | "geoparquet";

export function exportSelection(
  table: Table,
  indices: number[],
  layerKey: string,
  format: ExportFormat
): void {
  switch (format) {
    case "csv":
      exportCSV(table, indices, layerKey);
      break;
    case "geojson":
      exportGeoJSON(table, indices, layerKey);
      break;
    case "geoparquet":
      exportViaAPI(layerKey, indices);
      break;
  }
}

function exportCSV(table: Table, indices: number[], layerKey: string): void {
  const cols = table.schema.fields
    .map((f) => f.name)
    .filter((n) => n !== "geometry");

  const header = cols.join(",");
  const rows = indices.map((i) =>
    cols
      .map((col) => {
        const val = table.getChild(col)?.get(i);
        if (val == null) return "";
        const str = String(val);
        return str.includes(",") || str.includes('"')
          ? `"${str.replace(/"/g, '""')}"`
          : str;
      })
      .join(",")
  );

  download(
    [header, ...rows].join("\n"),
    `${layerKey.replace("/", "_")}.csv`,
    "text/csv"
  );
}

function exportGeoJSON(
  table: Table,
  indices: number[],
  layerKey: string
): void {
  const cols = table.schema.fields.map((f) => f.name);
  const geomCol = table.getChild("geometry");

  const features = indices.map((i) => {
    const properties: Record<string, unknown> = {};
    for (const col of cols) {
      if (col === "geometry") continue;
      const val = table.getChild(col)?.get(i);
      properties[col] = typeof val === "bigint" ? Number(val) : val;
    }

    let geometry: unknown = null;
    if (geomCol) {
      const geom = geomCol.get(i);
      if (geom && typeof geom.get === "function") {
        // GeoArrow Point (FixedSizeList[2])
        geometry = {
          type: "Point",
          coordinates: [geom.get(0), geom.get(1)],
        };
      }
    }

    return { type: "Feature" as const, properties, geometry };
  });

  const fc = { type: "FeatureCollection" as const, features };
  download(
    JSON.stringify(fc, null, 2),
    `${layerKey.replace("/", "_")}.geojson`,
    "application/geo+json"
  );
}

async function exportViaAPI(
  layerKey: string,
  indices: number[]
): Promise<void> {
  const [ns, layer] = layerKey.split("/");
  const resp = await fetch(`/api/export/${ns}/${layer}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ row_indices: indices }),
  });
  if (!resp.ok) {
    console.error("Export failed:", resp.status);
    return;
  }
  const blob = await resp.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${layerKey.replace("/", "_")}.parquet`;
  a.click();
  URL.revokeObjectURL(url);
}

function download(content: string, filename: string, mime: string): void {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
