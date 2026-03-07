/**
 * Distance and area measurement tools using MapLibre GeoJSON source.
 * Click to place vertices, double-click to finish, right-click to undo.
 */

import type maplibregl from "maplibre-gl";

export type MeasureMode = "distance" | "area" | "none";

export class MeasureTool {
  private mode: MeasureMode = "none";
  private points: [number, number][] = [];
  private sourceId = "measure-source";
  private lineLayerId = "measure-line";
  private fillLayerId = "measure-fill";
  private pointLayerId = "measure-points";
  private onUpdate: (text: string) => void;

  constructor(
    private map: maplibregl.Map,
    onUpdate: (text: string) => void
  ) {
    this.onUpdate = onUpdate;
  }

  /** Initialize source and layers — call after map style loads */
  initLayers(): void {
    if (this.map.getSource(this.sourceId)) return;

    this.map.addSource(this.sourceId, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });

    this.map.addLayer({
      id: this.lineLayerId,
      type: "line",
      source: this.sourceId,
      filter: ["==", "$type", "LineString"],
      paint: {
        "line-color": "#ff4444",
        "line-width": 2,
        "line-dasharray": [3, 2],
      },
    });

    this.map.addLayer({
      id: this.fillLayerId,
      type: "fill",
      source: this.sourceId,
      filter: ["==", "$type", "Polygon"],
      paint: {
        "fill-color": "rgba(255, 68, 68, 0.15)",
      },
    });

    this.map.addLayer({
      id: this.pointLayerId,
      type: "circle",
      source: this.sourceId,
      filter: ["==", "$type", "Point"],
      paint: {
        "circle-radius": 5,
        "circle-color": "#ff4444",
        "circle-stroke-color": "#fff",
        "circle-stroke-width": 2,
      },
    });
  }

  activate(mode: MeasureMode): void {
    this.clear();
    this.mode = mode;
    this.initLayers();
    this.map.getCanvas().style.cursor = "crosshair";
    this.map.on("click", this.handleClick);
    this.map.on("dblclick", this.handleDblClick);
    this.map.on("contextmenu", this.handleRightClick);
  }

  deactivate(): void {
    this.mode = "none";
    this.map.getCanvas().style.cursor = "";
    this.map.off("click", this.handleClick);
    this.map.off("dblclick", this.handleDblClick);
    this.map.off("contextmenu", this.handleRightClick);
    this.clear();
  }

  isActive(): boolean {
    return this.mode !== "none";
  }

  private handleClick = (e: maplibregl.MapMouseEvent): void => {
    this.points.push([e.lngLat.lng, e.lngLat.lat]);
    this.updateGeometry();
    this.updateMeasurement();
  };

  private handleDblClick = (e: maplibregl.MapMouseEvent): void => {
    e.preventDefault();
    this.updateMeasurement();
  };

  private handleRightClick = (e: maplibregl.MapMouseEvent): void => {
    e.preventDefault();
    if (this.points.length > 0) {
      this.points.pop();
      this.updateGeometry();
      this.updateMeasurement();
    }
  };

  private updateGeometry(): void {
    const features: GeoJSON.Feature[] = [];

    for (const pt of this.points) {
      features.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: pt },
        properties: {},
      });
    }

    if (this.points.length >= 2) {
      if (this.mode === "distance") {
        features.push({
          type: "Feature",
          geometry: { type: "LineString", coordinates: this.points },
          properties: {},
        });
      } else if (this.mode === "area" && this.points.length >= 3) {
        features.push({
          type: "Feature",
          geometry: {
            type: "Polygon",
            coordinates: [[...this.points, this.points[0]]],
          },
          properties: {},
        });
      }
    }

    (this.map.getSource(this.sourceId) as maplibregl.GeoJSONSource).setData({
      type: "FeatureCollection",
      features,
    });
  }

  private updateMeasurement(): void {
    if (this.points.length < 2) {
      this.onUpdate("");
      return;
    }

    if (this.mode === "distance") {
      const dist = this.haversineTotal();
      this.onUpdate(this.formatDistance(dist));
    } else if (this.mode === "area" && this.points.length >= 3) {
      const area = this.geodesicArea();
      this.onUpdate(this.formatArea(area));
    }
  }

  private haversineTotal(): number {
    let total = 0;
    for (let i = 1; i < this.points.length; i++) {
      total += this.haversine(this.points[i - 1], this.points[i]);
    }
    return total;
  }

  private haversine(a: [number, number], b: [number, number]): number {
    const R = 6371000;
    const dLat = ((b[1] - a[1]) * Math.PI) / 180;
    const dLon = ((b[0] - a[0]) * Math.PI) / 180;
    const lat1 = (a[1] * Math.PI) / 180;
    const lat2 = (b[1] * Math.PI) / 180;
    const h =
      Math.sin(dLat / 2) ** 2 +
      Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.asin(Math.sqrt(h));
  }

  private geodesicArea(): number {
    const R = 6371000;
    let area = 0;
    const pts = this.points;
    for (let i = 0; i < pts.length; i++) {
      const j = (i + 1) % pts.length;
      const lat1 = (pts[i][1] * Math.PI) / 180;
      const lat2 = (pts[j][1] * Math.PI) / 180;
      const dLon = ((pts[j][0] - pts[i][0]) * Math.PI) / 180;
      area += dLon * (2 + Math.sin(lat1) + Math.sin(lat2));
    }
    return Math.abs((area * R * R) / 2);
  }

  private formatDistance(meters: number): string {
    if (meters > 1000) {
      return `${(meters / 1000).toFixed(2)} km (${(meters * 3.28084).toFixed(0)} ft)`;
    }
    return `${meters.toFixed(1)} m (${(meters * 3.28084).toFixed(0)} ft)`;
  }

  private formatArea(sqMeters: number): string {
    if (sqMeters > 1_000_000) {
      return `${(sqMeters / 1_000_000).toFixed(3)} km\u00B2 (${(sqMeters / 4046.86).toFixed(1)} acres)`;
    }
    return `${sqMeters.toFixed(0)} m\u00B2 (${(sqMeters / 4046.86).toFixed(3)} acres)`;
  }

  private clear(): void {
    this.points = [];
    const src = this.map.getSource(this.sourceId) as
      | maplibregl.GeoJSONSource
      | undefined;
    if (src) {
      src.setData({ type: "FeatureCollection", features: [] });
    }
    this.onUpdate("");
  }
}
