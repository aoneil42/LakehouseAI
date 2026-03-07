/**
 * URL hash-based state persistence for shareable map views.
 * Format: #map=zoom/lat/lng&layers=ns1/layer1,ns2/layer2
 */

export interface UrlMapState {
  zoom: number;
  center: [number, number]; // [lng, lat]
  layers: string[];
}

export function readUrlState(): Partial<UrlMapState> {
  const hash = window.location.hash.slice(1);
  if (!hash) return {};

  const params = new URLSearchParams(hash);
  const state: Partial<UrlMapState> = {};

  const mapParam = params.get("map");
  if (mapParam) {
    const parts = mapParam.split("/").map(Number);
    if (parts.length === 3 && parts.every(Number.isFinite)) {
      state.zoom = parts[0];
      state.center = [parts[2], parts[1]]; // hash is zoom/lat/lng → [lng, lat]
    }
  }

  const layerParam = params.get("layers");
  if (layerParam) {
    state.layers = layerParam.split(",").filter(Boolean);
  }

  return state;
}

export function writeUrlState(state: UrlMapState): void {
  const mapStr = `${state.zoom.toFixed(2)}/${state.center[1].toFixed(5)}/${state.center[0].toFixed(5)}`;
  const parts = [`map=${mapStr}`];
  if (state.layers.length > 0) {
    parts.push(`layers=${state.layers.join(",")}`);
  }
  history.replaceState(null, "", `#${parts.join("&")}`);
}
