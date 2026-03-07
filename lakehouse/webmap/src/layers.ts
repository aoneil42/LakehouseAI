/**
 * Layer builders using standard deck.gl layers with binary data read directly
 * from Arrow typed arrays (zero-copy).  This replaces the @geoarrow/deck.gl-layers
 * package which is incompatible with deck.gl v9 (broken earcut, assertion
 * failures on PathLayer init, stale WebGL buffers after basemap switch).
 */

import {
  SolidPolygonLayer,
  PathLayer,
  ScatterplotLayer,
} from "@deck.gl/layers";
import type { Table } from "apache-arrow";
import type { Layer, Color, PickingInfo } from "@deck.gl/core";

// ---------------------------------------------------------------------------
// Geometry type detection from Arrow/GeoArrow metadata
// ---------------------------------------------------------------------------

export type GeomType = "point" | "line" | "polygon" | "unknown";

/**
 * Detect the geometry type from GeoArrow extension metadata on the geometry
 * column. Returns "point", "line", "polygon", or "unknown".
 */
export function detectGeomType(table: Table): GeomType {
  const geomField = table.schema.fields.find((f) => f.name === "geometry");
  if (!geomField) return "unknown";

  const extName =
    geomField.metadata.get("ARROW:extension:name")?.toLowerCase() ?? "";

  if (extName.includes("point")) return "point";
  if (extName.includes("linestring")) return "line";
  if (extName.includes("polygon")) return "polygon";
  return "unknown";
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
      props[field.name] = typeof val === "bigint" ? Number(val) : val;
    }
  }
  return props;
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
  id: string = "points"
): Layer {
  const geomCol = table.getChild("geometry")!;
  const batch = geomCol.data[0];

  // FixedSizeList[2]<Float64> → children[0].values
  const coordValues = batch.children[0].values as Float64Array;
  const numPoints = batch.length;

  return new ScatterplotLayer({
    id,
    data: {
      length: numPoints,
      attributes: {
        getPosition: { value: coordValues, size: 2 },
      },
    },
    visible,
    getFillColor: DEFAULT_COLOR,
    getRadius: 300,
    radiusMinPixels: 3,
    radiusMaxPixels: 15,
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
  id: string = "lines"
): Layer {
  const geomCol = table.getChild("geometry")!;
  const batch = geomCol.data[0];

  const pathOffsets = batch.valueOffsets; // Int32Array: path → coord index
  const pointData = batch.children[0]; // FixedSizeList[2]<Float64>
  const coordValues = pointData.children[0].values as Float64Array;
  const numPaths = batch.length;

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
    getColor: DEFAULT_LINE_COLOR,
    getWidth: 2,
    widthMinPixels: 1,
    widthMaxPixels: 5,
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
  id: string = "polygons"
): Layer {
  const geomCol = table.getChild("geometry")!;
  const batch = geomCol.data[0];

  // Navigate the nested List structure
  const polygonOffsets = batch.valueOffsets; // polygon → ring index
  const ringData = batch.children[0]; // List<FixedSizeList[2]<Float64>>
  const ringOffsets = ringData.valueOffsets; // ring → coord index
  const pointData = ringData.children[0]; // FixedSizeList[2]<Float64>
  const coordValues = pointData.children[0].values as Float64Array;
  const numPolygons = batch.length;

  // Check if ANY polygon has more than one ring (i.e. has holes)
  let hasHoles = false;
  for (let i = 0; i < numPolygons; i++) {
    if (polygonOffsets[i + 1] - polygonOffsets[i] > 1) {
      hasHoles = true;
      break;
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
      getFillColor: DEFAULT_POLYGON_FILL,
      _normalize: true,
      pickable: true,
      onClick: makePickHandler(table, onClick, id),
    });
  }

  // ── Accessor path: polygons with holes need nested ring arrays ──
  // Construct [outerRing, hole1, hole2, …] per polygon so earcut
  // gets explicit hole boundaries.  Simple polygons become [ring].
  const polygons: PolygonRings[] = new Array(numPolygons);
  for (let i = 0; i < numPolygons; i++) {
    const rStart = polygonOffsets[i];
    const rEnd = polygonOffsets[i + 1];
    const numRings = rEnd - rStart;

    if (numRings === 1) {
      // Simple polygon — single ring of [lng, lat] pairs
      const cStart = ringOffsets[rStart];
      const cEnd = ringOffsets[rStart + 1];
      const ring: number[][] = new Array(cEnd - cStart);
      for (let c = cStart; c < cEnd; c++) {
        ring[c - cStart] = [coordValues[c * 2], coordValues[c * 2 + 1]];
      }
      polygons[i] = ring;
    } else {
      // Polygon with holes — array of rings
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
    getFillColor: DEFAULT_POLYGON_FILL,
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
  id?: string
): Layer {
  const geomType = detectGeomType(table);
  switch (geomType) {
    case "point":
      return buildPointLayer(table, visible, onClick, id);
    case "line":
      return buildLineLayer(table, visible, onClick, id);
    case "polygon":
      return buildPolygonLayer(table, visible, onClick, id);
    default:
      // Fall back to polygon layer for unknown types
      return buildPolygonLayer(table, visible, onClick, id);
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
  id: string = "aggregate"
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
