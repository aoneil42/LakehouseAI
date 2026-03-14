/**
 * WKB (Well-Known Binary) parser for geometry columns.
 *
 * DuckDB 1.5 Arrow IPC export encodes geometry as Binary columns with
 * the "geoarrow.wkb" extension type.  The layer builders in layers.ts
 * expect native GeoArrow nested-list arrays (FixedSizeList, List, etc.).
 *
 * This module parses WKB bytes and produces the same typed arrays the
 * layer builders need — flat Float64Array of coordinates plus Int32Array
 * offset arrays for paths/rings/polygons.
 */

import type { Vector } from "apache-arrow";
import type { GeomType } from "./layers";

// WKB geometry type constants (lower byte only — mask out SRID flags)
const WKB_POINT = 1;
const WKB_LINESTRING = 2;
const WKB_POLYGON = 3;
const WKB_MULTIPOINT = 4;
const WKB_MULTILINESTRING = 5;
const WKB_MULTIPOLYGON = 6;

// ---------------------------------------------------------------------------
// Type detection
// ---------------------------------------------------------------------------

/** Read the WKB geometry type from a single WKB value. */
function wkbType(wkb: Uint8Array): number {
  const le = wkb[0] === 1;
  const dv = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength);
  return (le ? dv.getUint32(1, true) : dv.getUint32(1, false)) & 0xff;
}

/**
 * Detect geometry type from the first non-null WKB value in the column.
 */
export function detectWkbGeomType(geomCol: Vector): GeomType {
  for (let i = 0; i < geomCol.length; i++) {
    const val = geomCol.get(i);
    if (val != null && val.byteLength >= 5) {
      const t = wkbType(val);
      if (t === WKB_POINT || t === WKB_MULTIPOINT) return "point";
      if (t === WKB_LINESTRING || t === WKB_MULTILINESTRING) return "line";
      if (t === WKB_POLYGON || t === WKB_MULTIPOLYGON) return "polygon";
    }
  }
  return "unknown";
}

// ---------------------------------------------------------------------------
// Point parsing
// ---------------------------------------------------------------------------

export interface WkbPointData {
  coords: Float64Array; // flat [x0,y0, x1,y1, ...]
  numPoints: number;
}

/** Parse WKB Point/MultiPoint column into flat coordinate array. */
export function parseWkbPoints(geomCol: Vector): WkbPointData {
  const n = geomCol.length;

  // First pass: count total points
  let totalPoints = 0;
  for (let i = 0; i < n; i++) {
    const wkb = geomCol.get(i) as Uint8Array | null;
    if (!wkb || wkb.byteLength < 5) { totalPoints++; continue; } // null → origin
    const t = wkbType(wkb);
    if (t === WKB_POINT) {
      totalPoints++;
    } else if (t === WKB_MULTIPOINT) {
      const le = wkb[0] === 1;
      const dv = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength);
      totalPoints += le ? dv.getUint32(5, true) : dv.getUint32(5, false);
    } else {
      totalPoints++; // fallback
    }
  }

  const coords = new Float64Array(totalPoints * 2);
  let ci = 0;

  for (let i = 0; i < n; i++) {
    const wkb = geomCol.get(i) as Uint8Array | null;
    if (!wkb || wkb.byteLength < 21) {
      coords[ci++] = 0;
      coords[ci++] = 0;
      continue;
    }
    const le = wkb[0] === 1;
    const dv = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength);
    const t = wkbType(wkb);

    if (t === WKB_POINT) {
      coords[ci++] = dv.getFloat64(5, le);
      coords[ci++] = dv.getFloat64(13, le);
    } else if (t === WKB_MULTIPOINT) {
      const numPts = le ? dv.getUint32(5, true) : dv.getUint32(5, false);
      let off = 9;
      for (let p = 0; p < numPts; p++) {
        const ple = wkb[off] === 1;
        const pdv = new DataView(wkb.buffer, wkb.byteOffset + off, wkb.byteLength - off);
        coords[ci++] = pdv.getFloat64(5, ple);
        coords[ci++] = pdv.getFloat64(13, ple);
        off += 21; // 1 + 4 + 8 + 8
      }
    } else {
      coords[ci++] = 0;
      coords[ci++] = 0;
    }
  }

  return { coords, numPoints: totalPoints };
}

// ---------------------------------------------------------------------------
// LineString parsing
// ---------------------------------------------------------------------------

export interface WkbLineData {
  pathOffsets: Int32Array; // path → coord index
  coords: Float64Array;   // flat [x0,y0, x1,y1, ...]
  numPaths: number;
}

/** Parse WKB LineString/MultiLineString column. */
export function parseWkbLines(geomCol: Vector): WkbLineData {
  const n = geomCol.length;

  // First pass: count paths and total coordinates
  let totalPaths = 0;
  let totalCoords = 0;

  for (let i = 0; i < n; i++) {
    const wkb = geomCol.get(i) as Uint8Array | null;
    if (!wkb || wkb.byteLength < 5) continue;
    const t = wkbType(wkb);
    const le = wkb[0] === 1;
    const dv = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength);

    if (t === WKB_LINESTRING) {
      totalPaths++;
      totalCoords += le ? dv.getUint32(5, true) : dv.getUint32(5, false);
    } else if (t === WKB_MULTILINESTRING) {
      const numLines = le ? dv.getUint32(5, true) : dv.getUint32(5, false);
      let off = 9;
      for (let l = 0; l < numLines; l++) {
        const lle = wkb[off] === 1;
        const ldv = new DataView(wkb.buffer, wkb.byteOffset + off, wkb.byteLength - off);
        const numPts = lle ? ldv.getUint32(5, true) : ldv.getUint32(5, false);
        totalPaths++;
        totalCoords += numPts;
        off += 9 + numPts * 16;
      }
    }
  }

  const pathOffsets = new Int32Array(totalPaths + 1);
  const coords = new Float64Array(totalCoords * 2);
  let pi = 0; // path index
  let ci = 0; // coord index (into coords array, counts coordinate pairs)

  for (let i = 0; i < n; i++) {
    const wkb = geomCol.get(i) as Uint8Array | null;
    if (!wkb || wkb.byteLength < 5) continue;
    const t = wkbType(wkb);
    const le = wkb[0] === 1;
    const dv = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength);

    if (t === WKB_LINESTRING) {
      const numPts = le ? dv.getUint32(5, true) : dv.getUint32(5, false);
      pathOffsets[pi] = ci;
      for (let p = 0; p < numPts; p++) {
        const off = 9 + p * 16;
        coords[ci * 2] = dv.getFloat64(off, le);
        coords[ci * 2 + 1] = dv.getFloat64(off + 8, le);
        ci++;
      }
      pi++;
    } else if (t === WKB_MULTILINESTRING) {
      const numLines = le ? dv.getUint32(5, true) : dv.getUint32(5, false);
      let off = 9;
      for (let l = 0; l < numLines; l++) {
        const lle = wkb[off] === 1;
        const ldv = new DataView(wkb.buffer, wkb.byteOffset + off, wkb.byteLength - off);
        const numPts = lle ? ldv.getUint32(5, true) : ldv.getUint32(5, false);
        pathOffsets[pi] = ci;
        for (let p = 0; p < numPts; p++) {
          const coff = 9 + p * 16;
          coords[ci * 2] = ldv.getFloat64(coff, lle);
          coords[ci * 2 + 1] = ldv.getFloat64(coff + 8, lle);
          ci++;
        }
        pi++;
        off += 9 + numPts * 16;
      }
    }
  }
  pathOffsets[pi] = ci;

  return { pathOffsets, coords, numPaths: totalPaths };
}

// ---------------------------------------------------------------------------
// Polygon parsing
// ---------------------------------------------------------------------------

export interface WkbPolygonData {
  polygonOffsets: Int32Array; // polygon → ring index
  ringOffsets: Int32Array;    // ring → coord index
  coords: Float64Array;      // flat [x0,y0, x1,y1, ...]
  numPolygons: number;
  hasHoles: boolean;
}

/** Parse WKB Polygon/MultiPolygon column. */
export function parseWkbPolygons(geomCol: Vector): WkbPolygonData {
  const n = geomCol.length;

  // First pass: count polygons, rings, and coordinates
  let totalPolygons = 0;
  let totalRings = 0;
  let totalCoords = 0;
  let hasHoles = false;

  for (let i = 0; i < n; i++) {
    const wkb = geomCol.get(i) as Uint8Array | null;
    if (!wkb || wkb.byteLength < 5) continue;
    const t = wkbType(wkb);
    const le = wkb[0] === 1;
    const dv = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength);

    if (t === WKB_POLYGON) {
      totalPolygons++;
      const numRings = le ? dv.getUint32(5, true) : dv.getUint32(5, false);
      if (numRings > 1) hasHoles = true;
      totalRings += numRings;
      let off = 9;
      for (let r = 0; r < numRings; r++) {
        const numPts = le ? dv.getUint32(off, true) : dv.getUint32(off, false);
        totalCoords += numPts;
        off += 4 + numPts * 16;
      }
    } else if (t === WKB_MULTIPOLYGON) {
      const numPolys = le ? dv.getUint32(5, true) : dv.getUint32(5, false);
      let off = 9;
      for (let g = 0; g < numPolys; g++) {
        totalPolygons++;
        const gle = wkb[off] === 1;
        const gdv = new DataView(wkb.buffer, wkb.byteOffset + off, wkb.byteLength - off);
        const numRings = gle ? gdv.getUint32(5, true) : gdv.getUint32(5, false);
        if (numRings > 1) hasHoles = true;
        totalRings += numRings;
        let roff = 9;
        for (let r = 0; r < numRings; r++) {
          const numPts = gle ? gdv.getUint32(roff, true) : gdv.getUint32(roff, false);
          totalCoords += numPts;
          roff += 4 + numPts * 16;
        }
        off += roff;
      }
    }
  }

  const polygonOffsets = new Int32Array(totalPolygons + 1);
  const ringOffsets = new Int32Array(totalRings + 1);
  const coords = new Float64Array(totalCoords * 2);
  let pgi = 0; // polygon index
  let ri = 0;  // ring index
  let ci = 0;  // coord pair index

  for (let i = 0; i < n; i++) {
    const wkb = geomCol.get(i) as Uint8Array | null;
    if (!wkb || wkb.byteLength < 5) continue;
    const t = wkbType(wkb);
    const le = wkb[0] === 1;
    const dv = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength);

    if (t === WKB_POLYGON) {
      polygonOffsets[pgi] = ri;
      const numRings = le ? dv.getUint32(5, true) : dv.getUint32(5, false);
      let off = 9;
      for (let r = 0; r < numRings; r++) {
        ringOffsets[ri] = ci;
        const numPts = le ? dv.getUint32(off, true) : dv.getUint32(off, false);
        off += 4;
        for (let p = 0; p < numPts; p++) {
          coords[ci * 2] = dv.getFloat64(off, le);
          coords[ci * 2 + 1] = dv.getFloat64(off + 8, le);
          ci++;
          off += 16;
        }
        ri++;
      }
      pgi++;
    } else if (t === WKB_MULTIPOLYGON) {
      const numPolys = le ? dv.getUint32(5, true) : dv.getUint32(5, false);
      let off = 9;
      for (let g = 0; g < numPolys; g++) {
        polygonOffsets[pgi] = ri;
        const gle = wkb[off] === 1;
        const gdv = new DataView(wkb.buffer, wkb.byteOffset + off, wkb.byteLength - off);
        const numRings = gle ? gdv.getUint32(5, true) : gdv.getUint32(5, false);
        let roff = 9;
        for (let r = 0; r < numRings; r++) {
          ringOffsets[ri] = ci;
          const numPts = gle ? gdv.getUint32(roff, true) : gdv.getUint32(roff, false);
          roff += 4;
          for (let p = 0; p < numPts; p++) {
            coords[ci * 2] = gdv.getFloat64(roff, gle);
            coords[ci * 2 + 1] = gdv.getFloat64(roff + 8, gle);
            ci++;
            roff += 16;
          }
          ri++;
        }
        off += roff;
        pgi++;
      }
    }
  }
  polygonOffsets[pgi] = ri;
  ringOffsets[ri] = ci;

  return { polygonOffsets, ringOffsets, coords, numPolygons: totalPolygons, hasHoles };
}
