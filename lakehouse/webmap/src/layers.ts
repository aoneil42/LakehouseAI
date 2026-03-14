/**
 * Layer builders using standard deck.gl layers with binary data read directly
 * from Arrow typed arrays (zero-copy).  This replaces the @geoarrow/deck.gl-layers
 * package which is incompatible with deck.gl v9 (broken earcut, assertion
 * failures on PathLayer init, stale WebGL buffers after basemap switch).
 */

import { DataType } from "apache-arrow";
import {
  SolidPolygonLayer,
  PathLayer,
  ScatterplotLayer,
} from "@deck.gl/layers";
import type { Table } from "apache-arrow";
import type { Layer, Color, PickingInfo } from "@deck.gl/core";
import type { LayerStyle } from "./symbology";
import {
  detectWkbGeomType,
  parseWkbPoints,
  parseWkbLines,
  parseWkbPolygons,
} from "./wkb";

// ---------------------------------------------------------------------------
// Geometry type detection from Arrow/GeoArrow metadata
// ---------------------------------------------------------------------------

export type GeomType = "point" | "line" | "polygon" | "unknown";

/**
 * Detect the geometry type from GeoArrow extension metadata on the geometry
 * column. Returns "point", "line", "polygon", or "unknown".
 *
 * For native GeoArrow types the extension name encodes the geometry type
 * (e.g. "geoarrow.point", "geoarrow.linestring").  For WKB-encoded columns
 * ("geoarrow.wkb") we peek at the first WKB value to determine the type.
 */
export function detectGeomType(table: Table): GeomType {
  const geomField = table.schema.fields.find((f) => f.name === "geometry");
  if (!geomField) return "unknown";

  const extName =
    geomField.metadata.get("ARROW:extension:name")?.toLowerCase() ?? "";

  if (extName.includes("point")) return "point";
  if (extName.includes("linestring")) return "line";
  if (extName.includes("polygon")) return "polygon";

  // WKB-encoded geometry — inspect actual bytes to determine type
  if (extName === "geoarrow.wkb" || extName === "") {
    const geomCol = table.getChild("geometry");
    if (geomCol) return detectWkbGeomType(geomCol);
  }

  return "unknown";
}

/** Check if the geometry column is WKB-encoded (Binary type). */
function isWkbGeometry(table: Table): boolean {
  const geomField = table.schema.fields.find((f) => f.name === "geometry");
  if (!geomField) return false;
  const extName =
    geomField.metadata.get("ARROW:extension:name")?.toLowerCase() ?? "";
  return extName === "geoarrow.wkb" || (extName === "" && DataType.isBinary(geomField.type));
}

// ---------------------------------------------------------------------------
// Color palettes
// ---------------------------------------------------------------------------

const CATEGORY_COLORS: Record<string, Color> = {
  park: [34, 139, 34, 255],
  school: [30, 144, 255, 255],
  hospital: [220, 20, 60, 255],
  restaurant: [255, 165, 0, 255],
  gas_station: [128, 128, 128, 255],
  trailhead: [107, 142, 35, 255],
  campground: [139, 69, 19, 255],
  viewpoint: [148, 103, 189, 255],
  water_tower: [0, 191, 255, 255],
  fire_station: [255, 69, 0, 255],
};

const PARCEL_COLORS: Record<string, Color> = {
  residential: [65, 105, 225, 100],
  commercial: [255, 140, 0, 100],
  industrial: [169, 169, 169, 100],
  agricultural: [34, 139, 34, 100],
  public: [148, 103, 189, 100],
};

const DEFAULT_COLOR: Color = [100, 100, 100, 255];

const DEFAULT_LINE_COLOR: Color = [80, 80, 80, 200];

/** Default polygon fill — visible blue at 63% opacity */
const DEFAULT_POLYGON_FILL: Color = [30, 144, 255, 160];

export type FeatureClickHandler = (info: Record<string, unknown>) => void;

// ---------------------------------------------------------------------------
// Picking — look up Arrow table properties by row index
// ---------------------------------------------------------------------------

/**
 * Build a pick handler that reads properties from the Arrow table at the
 * picked row index.  Works with deck.gl's binary data mode where info.object
 * is undefined but info.index gives the row.
 */
function makePickHandler(
  table: Table,
  onClick?: FeatureClickHandler,
  layerId?: string
): ((info: PickingInfo) => void) | undefined {
  if (!onClick) return undefined;
  return (info: PickingInfo) => {
    if (info.index < 0) return;
    const idx = info.index;
    const props: Record<string, unknown> = {
      type: layerId ?? info.layer?.id ?? "",
    };
    for (const field of table.schema.fields) {
      if (field.name === "geometry") continue;
      const col = table.getChild(field.name);
      if (col) {
        const val = col.get(idx);
        // Coerce BigInt to Number for display
        props[field.name] =
          typeof val === "bigint" ? Number(val) : val;
      }
    }
    onClick(props);
  };
}

/**
 * Look up feature properties from an Arrow table by row index.
 * Used by identify/box-select which receives raw PickingInfo.
 */
export function getFeatureProps(
  table: Table,
  index: number,
  layerId: string
): Record<string, unknown> {
  const props: Record<string, unknown> = { type: layerId };
  for (const field of table.schema.fields) {
    if (field.name === "geometry") continue;
    const col = table.getChild(field.name);
    if (col) {
      const val = col.get(index);
      props[field.name] = formatPropValue(field, val);
    }
  }
  return props;
}

/** Format a single property value, converting temporal types to readable strings. */
function formatPropValue(field: { name: string; type: any }, val: unknown): unknown {
  if (val == null) return val;

  // Arrow Timestamp → human-readable
  if (DataType.isTimestamp(field.type)) {
    const n = typeof val === "bigint" ? Number(val) : Number(val);
    const units = [1000, 1, 0.001, 0.000001]; // s, ms, us, ns → ms multiplier
    const ms = n * (units[field.type.unit] ?? 1);
    const d = new Date(ms);
    return isNaN(d.getTime()) ? n : d.toISOString().replace("T", " ").slice(0, 19);
  }
  // Arrow Date → YYYY-MM-DD
  if (DataType.isDate(field.type)) {
    const n = typeof val === "bigint" ? Number(val) : Number(val);
    const ms = field.type.typeId === -14 ? n : n * 86_400_000; // DateMillisecond vs DateDay
    const d = new Date(ms);
    return isNaN(d.getTime()) ? n : d.toISOString().slice(0, 10);
  }
  // Heuristic: BIGINT columns with timestamp-like names
  if (typeof val === "bigint") {
    if (/timestamp|created|updated|modified|_at$|_time$|_date$|datetime|epoch/i.test(field.name)) {
      const n = Number(val);
      if (n > 631_152_000_000 && n < 4_102_444_800_000) {
        const d = new Date(n);
        if (!isNaN(d.getTime())) return d.toISOString().replace("T", " ").slice(0, 19);
      }
    }
    return Number(val);
  }
  return val;
}

// ---------------------------------------------------------------------------
// Layer builders — read Arrow geometry buffers directly (zero-copy)
// ---------------------------------------------------------------------------

/**
 * Build a ScatterplotLayer for GeoArrow point geometry.
 *
 * Arrow structure: FixedSizeList[2]<Float64>
 *   → children[0].values = flat Float64Array [x0, y0, x1, y1, …]
 */
export function buildPointLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id: string = "points",
  opacity: number = 1.0,
  style?: LayerStyle
): Layer {
  const geomCol = table.getChild("geometry")!;

  let coordValues: Float64Array;
  let numPoints: number;

  if (isWkbGeometry(table)) {
    const parsed = parseWkbPoints(geomCol);
    coordValues = parsed.coords;
    numPoints = parsed.numPoints;
  } else {
    // Native GeoArrow: FixedSizeList[2]<Float64> → children[0].values
    const batch = geomCol.data[0];
    coordValues = batch.children[0].values as Float64Array;
    numPoints = batch.length;
  }

  const radiusPx = style?.radius ?? 5;

  return new ScatterplotLayer({
    id,
    data: {
      length: numPoints,
      attributes: {
        getPosition: { value: coordValues, size: 2 },
      },
    },
    visible,
    opacity: style?.opacity ?? opacity,
    getFillColor: style?.fillColor ?? DEFAULT_COLOR,
    getLineColor: style?.strokeColor,
    radiusUnits: "pixels" as const,
    getRadius: radiusPx,
    lineWidthMinPixels: style?.strokeWidth ?? 1,
    stroked: true,
    radiusMinPixels: 1,
    radiusMaxPixels: 50,
    pickable: true,
    onClick: makePickHandler(table, onClick, id),
  });
}

/**
 * Build a PathLayer for GeoArrow linestring geometry.
 *
 * Arrow structure: List<FixedSizeList[2]<Float64>>
 *   L0 valueOffsets → path start/end indices into the point array
 *   L1 children[0]  → FixedSizeList[2]<Float64>
 *   L2 children[0].values → flat Float64Array
 */
export function buildLineLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id: string = "lines",
  opacity: number = 1.0,
  style?: LayerStyle
): Layer {
  const geomCol = table.getChild("geometry")!;

  let pathOffsets: Int32Array;
  let coordValues: Float64Array;
  let numPaths: number;

  if (isWkbGeometry(table)) {
    const parsed = parseWkbLines(geomCol);
    pathOffsets = parsed.pathOffsets;
    coordValues = parsed.coords;
    numPaths = parsed.numPaths;
  } else {
    // Native GeoArrow: List<FixedSizeList[2]<Float64>>
    const batch = geomCol.data[0];
    pathOffsets = batch.valueOffsets;
    const pointData = batch.children[0];
    coordValues = pointData.children[0].values as Float64Array;
    numPaths = batch.length;
  }

  const widthPx = style?.strokeWidth ?? 2;

  return new PathLayer({
    id,
    data: {
      length: numPaths,
      startIndices: pathOffsets,
      attributes: {
        getPath: { value: coordValues, size: 2 },
      },
    },
    visible,
    opacity: style?.opacity ?? opacity,
    getColor: style?.fillColor ?? DEFAULT_LINE_COLOR,
    getWidth: widthPx,
    widthUnits: "pixels" as const,
    widthMinPixels: 1,
    widthMaxPixels: 50,
    pickable: true,
    onClick: makePickHandler(table, onClick, id),
  });
}

/** A polygon is an array of rings; each ring is an array of [lng, lat] pairs.
 *  Simple polygons (no holes) can be a single ring: [[x,y], …].
 *  Polygons with holes are nested: [outerRing, hole1, hole2, …]. */
type PolygonRings = number[][] | number[][][];

/**
 * Build a SolidPolygonLayer for GeoArrow polygon geometry.
 *
 * Arrow structure: List<List<FixedSizeList[2]<Float64>>>
 *   L0 valueOffsets → polygon start/end indices into the ring array
 *   L1 children[0].valueOffsets → ring start/end into the point array
 *   L2 children[0].children[0] → FixedSizeList[2]<Float64>
 *   L3 children[0].values → flat Float64Array
 *
 * deck.gl's binary data path does NOT pass hole boundaries to earcut,
 * so polygons with inner rings (holes) produce garbled triangulation.
 *
 * Strategy: detect whether ANY polygon has holes.
 *   • All simple (1 ring each) → fast binary path with startIndices.
 *   • Any holes present → accessor path with nested ring arrays so
 *     earcut receives explicit hole boundaries.
 */
export function buildPolygonLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id: string = "polygons",
  opacity: number = 1.0,
  style?: LayerStyle
): Layer {
  const geomCol = table.getChild("geometry")!;

  let polygonOffsets: Int32Array;
  let ringOffsets: Int32Array;
  let coordValues: Float64Array;
  let numPolygons: number;
  let hasHoles: boolean;

  if (isWkbGeometry(table)) {
    const parsed = parseWkbPolygons(geomCol);
    polygonOffsets = parsed.polygonOffsets;
    ringOffsets = parsed.ringOffsets;
    coordValues = parsed.coords;
    numPolygons = parsed.numPolygons;
    hasHoles = parsed.hasHoles;
  } else {
    // Native GeoArrow: List<List<FixedSizeList[2]<Float64>>>
    const batch = geomCol.data[0];
    polygonOffsets = batch.valueOffsets;
    const ringData = batch.children[0];
    ringOffsets = ringData.valueOffsets;
    const pointData = ringData.children[0];
    coordValues = pointData.children[0].values as Float64Array;
    numPolygons = batch.length;

    hasHoles = false;
    for (let i = 0; i < numPolygons; i++) {
      if (polygonOffsets[i + 1] - polygonOffsets[i] > 1) {
        hasHoles = true;
        break;
      }
    }
  }

  if (!hasHoles) {
    // ── Fast binary path: all simple polygons, no holes ──────────
    const startIndices = new Int32Array(numPolygons + 1);
    for (let i = 0; i <= numPolygons; i++) {
      startIndices[i] = ringOffsets[polygonOffsets[i]];
    }
    return new SolidPolygonLayer({
      id,
      data: {
        length: numPolygons,
        startIndices,
        attributes: {
          getPolygon: { value: coordValues, size: 2 },
        },
      },
      visible,
      opacity: style?.opacity ?? opacity,
      getFillColor: style?.fillColor ?? DEFAULT_POLYGON_FILL,
      _normalize: true,
      pickable: true,
      onClick: makePickHandler(table, onClick, id),
    });
  }

  // ── Accessor path: polygons with holes need nested ring arrays ──
  const polygons: PolygonRings[] = new Array(numPolygons);
  for (let i = 0; i < numPolygons; i++) {
    const rStart = polygonOffsets[i];
    const rEnd = polygonOffsets[i + 1];
    const numRings = rEnd - rStart;

    if (numRings === 1) {
      const cStart = ringOffsets[rStart];
      const cEnd = ringOffsets[rStart + 1];
      const ring: number[][] = new Array(cEnd - cStart);
      for (let c = cStart; c < cEnd; c++) {
        ring[c - cStart] = [coordValues[c * 2], coordValues[c * 2 + 1]];
      }
      polygons[i] = ring;
    } else {
      const rings: number[][][] = new Array(numRings);
      for (let r = 0; r < numRings; r++) {
        const cStart = ringOffsets[rStart + r];
        const cEnd = ringOffsets[rStart + r + 1];
        const ring: number[][] = new Array(cEnd - cStart);
        for (let c = cStart; c < cEnd; c++) {
          ring[c - cStart] = [coordValues[c * 2], coordValues[c * 2 + 1]];
        }
        rings[r] = ring;
      }
      polygons[i] = rings;
    }
  }

  return new SolidPolygonLayer({
    id,
    data: polygons,
    getPolygon: (d: PolygonRings) => d as any,
    visible,
    opacity: style?.opacity ?? opacity,
    getFillColor: style?.fillColor ?? DEFAULT_POLYGON_FILL,
    _normalize: true,
    pickable: true,
    onClick: makePickHandler(table, onClick, id),
  });
}

// ---------------------------------------------------------------------------
// Auto layer — picks the right builder based on geometry type
// ---------------------------------------------------------------------------

/**
 * Build the appropriate deck.gl layer based on the geometry type detected
 * from the Arrow table's GeoArrow extension metadata.
 */
export function buildAutoLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id?: string,
  opacity: number = 1.0,
  style?: LayerStyle
): Layer {
  const geomType = detectGeomType(table);
  switch (geomType) {
    case "point":
      return buildPointLayer(table, visible, onClick, id, opacity, style);
    case "line":
      return buildLineLayer(table, visible, onClick, id, opacity, style);
    case "polygon":
      return buildPolygonLayer(table, visible, onClick, id, opacity, style);
    default:
      // Fall back to polygon layer for unknown types
      return buildPolygonLayer(table, visible, onClick, id, opacity, style);
  }
}

// ---------------------------------------------------------------------------
// Aggregate bubble layer — grid-binned centroids with feature counts
// ---------------------------------------------------------------------------

/** Semi-transparent orange for cluster bubbles */
const AGGREGATE_FILL: Color = [255, 140, 0, 180];
const AGGREGATE_STROKE: Color = [200, 100, 0, 255];

interface AggregateRow {
  position: [number, number];
  count: number;
}

/**
 * Build a bubble-map layer for server-aggregated (grid-binned) data.
 * The input table has columns: geometry (GeoArrow Point), feature_count (int).
 */
export function buildAggregateLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id: string = "aggregate",
  opacity: number = 1.0
): Layer {
  const n = table.numRows;
  const geomCol = table.getChild("geometry");
  const countCol = table.getChild("feature_count");

  if (!geomCol || !countCol || n === 0) {
    return new ScatterplotLayer<AggregateRow>({ id, data: [], visible });
  }

  // Extract positions and counts from the Arrow table.
  // GeoArrow Point geometry is a FixedSizeList[2]<Float64>; access via .get(0/1).
  // If WKB-encoded instead, coords are at byte offsets 5 (x) and 13 (y).
  const rows: AggregateRow[] = new Array(n);
  let maxCount = 1;

  for (let i = 0; i < n; i++) {
    const geom = geomCol.get(i);
    let x = 0,
      y = 0;
    if (geom != null) {
      if (typeof geom.get === "function") {
        // GeoArrow FixedSizeList
        x = geom.get(0);
        y = geom.get(1);
      } else if (geom instanceof Uint8Array && geom.byteLength >= 21) {
        // WKB fallback — Point: byte-order(1) + type(4) + x(8) + y(8)
        const dv = new DataView(
          geom.buffer,
          geom.byteOffset,
          geom.byteLength
        );
        const le = geom[0] === 1;
        x = dv.getFloat64(5, le);
        y = dv.getFloat64(13, le);
      }
    }
    const raw = countCol.get(i) ?? 1;
    // DuckDB COUNT(*) produces BigInt64 in Arrow — coerce to Number
    const c = typeof raw === "bigint" ? Number(raw) : (raw as number);
    rows[i] = { position: [x, y], count: c };
    if (c > maxCount) maxCount = c;
  }

  const sqrtMax = Math.sqrt(maxCount);

  return new ScatterplotLayer<AggregateRow>({
    id,
    data: rows,
    visible,
    opacity,
    getPosition: (d) => d.position,
    getRadius: (d) => {
      // Radius proportional to sqrt(count) for perceptual area scaling
      const normalized = Math.sqrt(d.count) / sqrtMax;
      return 4000 + normalized * 46000;
    },
    getFillColor: AGGREGATE_FILL,
    getLineColor: AGGREGATE_STROKE,
    stroked: true,
    lineWidthMinPixels: 1,
    radiusMinPixels: 6,
    radiusMaxPixels: 40,
    pickable: true,
    onClick: (info: PickingInfo) => {
      if (!onClick || !info.object) return;
      const obj = info.object as AggregateRow;
      onClick({
        type: `${id} (aggregated)`,
        feature_count: obj.count,
        longitude: obj.position[0],
        latitude: obj.position[1],
      });
    },
  });
}
