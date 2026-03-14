import "./style.css";
import {
  loadBasemapConfig,
  initMap,
  setLayers,
  getMap,
  getOverlay,
  getBasemaps,
  getCurrentBasemapIndex,
  switchBasemap,
  pickObjectsInRect,
  flyToBounds,
  resetView,
  getViewportBbox,
  onMoveEnd,
} from "./map";
import {
  loadLayer,
  fetchNamespaces,
  fetchNamespaceTree,
  fetchTables,
  fetchTableBbox,
  fetchSchema,
  expandBbox,
  MAX_FEATURES_PER_LAYER,
} from "./queries";
import type { Bbox, TimeFilter } from "./queries";
import { buildAutoLayer, detectGeomType, getFeatureProps } from "./layers";
import type { FeatureClickHandler, GeomType } from "./layers";
import type { Table, Vector } from "apache-arrow";
import { readUrlState, writeUrlState } from "./url-state";
import { AttributeTable } from "./attribute-table";
import { MeasureTool } from "./measure";
import { captureMap } from "./screenshot";
import { SymbologyPanel, getDefaultStyle } from "./symbology";
import type { LayerStyle } from "./symbology";
import { TimeSlider } from "./time-slider";
import type { TimeConfig } from "./time-slider";
import {
  initCatalogBrowser,
  buildCatalogTree,
  updateTreeLayerCount,
  setTreeLayerLoading,
  setTreeLayerChecked,
  setStatus,
  showPopup,
  hidePopup,
  initIdentifyToggle,
  initBasemapPicker,
  showIdentifyPanel,
  hideIdentifyPanel,
  addIdentifyResult,
  clearIdentifyResults,
  deactivateIdentifyButton,
  showSearchResults,
  hideSearchResults,
  deactivateMeasureButtons,
  initActiveLayers,
  renderActiveLayers,
  initCatalogModal,
  openCatalogModal,
  debugLog,
} from "./ui";
import type { SearchResult, ActiveLayerInfo } from "./ui";
import { showSaveDialog } from "./save-dialog";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/** Loaded Arrow tables keyed by "namespace/layer" */
const tables = new Map<string, Table>();
/** Detected geometry type per loaded table key */
const geomTypes = new Map<string, GeomType>();
/** Set of currently visible layer keys ("namespace/layer") */
const visibleSet = new Set<string>();
/** Viewport bbox used for the most recent load, per layer key */
const loadedBbox = new Map<string, Bbox | undefined>();
/** Keys currently being loaded — prevents moveend from re-triggering loads. */
const loadingKeys = new Set<string>();
/** Cooldown guard for polygon layers — prevents moveend from reloading too
 *  frequently while the layer is being triangulated. */
const earcutCooldownKeys = new Set<string>();
/** Zoom level at which each layer was last loaded */
const loadedZoom = new Map<string, number>();
/** Per-layer style (fill, stroke, opacity, radius) */
const layerStyles = new Map<string, LayerStyle>();
/** Active attribute table instance */
let activeAttrTable: AttributeTable | null = null;
/** Active symbology panel instance */
let activeSymPanel: SymbologyPanel | null = null;
/** Measure tool instance — created after map init */
let measureTool: MeasureTool | null = null;
/** Active time sliders per layer key */
const activeTimeSliders = new Map<string, TimeSlider>();
/** Active time filters per layer key */
const timeFilters = new Map<string, TimeFilter>();
/** User-defined layer order (keys in draw order, bottom-first) */
let userLayerOrder: string[] | null = null;

let identifyActive = false;
let lastMouseX = 0;
let lastMouseY = 0;

// ---------------------------------------------------------------------------
// Layer rebuild
// ---------------------------------------------------------------------------

/** Draw-order rank: polygons at bottom, lines in middle, points on top */
const GEOM_ORDER: Record<GeomType, number> = {
  polygon: 0,
  unknown: 1,
  line: 2,
  point: 3,
};

function rebuildLayers() {
  // Build layers for ALL loaded tables — use visible flag to show/hide.
  // This avoids stale WebGL framebuffer artifacts from GeoArrow polygon
  // layers when they're removed from the array in non-interleaved mode.
  const allKeys = [...tables.keys()];
  if (userLayerOrder && userLayerOrder.length > 0) {
    // Use user-defined order, then add any new layers at the end
    const orderMap = new Map(userLayerOrder.map((k, i) => [k, i]));
    allKeys.sort((a, b) => {
      const oa = orderMap.get(a) ?? 999;
      const ob = orderMap.get(b) ?? 999;
      return oa - ob;
    });
  } else {
    allKeys.sort((a, b) => {
      const ga = geomTypes.get(a) ?? "unknown";
      const gb = geomTypes.get(b) ?? "unknown";
      return GEOM_ORDER[ga] - GEOM_ORDER[gb];
    });
  }

  const visibleCount = [...visibleSet].filter((k) => tables.has(k)).length;
  debugLog(`rebuildLayers: ${visibleCount} visible of ${allKeys.length} loaded: ${allKeys.filter((k) => visibleSet.has(k)).map((k) => `${k}(${geomTypes.get(k)})`).join(", ")}`);

  const layers: ReturnType<typeof buildAutoLayer>[] = [];
  for (const key of allKeys) {
    try {
      const t = tables.get(key)!;
      const vis = visibleSet.has(key);
      const style = layerStyles.get(key);
      const opacity = style?.opacity ?? 1.0;
      if (vis) debugLog(`  building ${key}: ${t.numRows} rows, ${t.batches.length} batches`);
      const l = buildAutoLayer(t, vis, handleClick, key, opacity, style);
      if (vis) debugLog(`  → ${key}: ${l.constructor.name} created OK`);
      layers.push(l);
    } catch (e) {
      debugLog(`  → ${key}: FAILED: ${(e as Error).message}`, "err");
      setStatus(`Error building ${key}: ${(e as Error).message}`);
    }
  }

  debugLog(`setLayers(${layers.length} layers, ${visibleCount} visible)`);
  setLayers(layers);

  // Check sub-layer state after earcut should have completed
  for (const key of allKeys) {
    const gt = geomTypes.get(key);
    if (gt === "polygon" && visibleSet.has(key)) {
      debugCheckDeckLayers(key);
    }
  }

  // Update the active layers legend
  updateActiveLayers();
}

// ---------------------------------------------------------------------------
// Status bar
// ---------------------------------------------------------------------------

const GEOM_ABBREV: Record<GeomType, string> = {
  point: "pts",
  line: "lines",
  polygon: "polys",
  unknown: "feat",
};

function updateStatusBar() {
  let total = 0;
  const parts: string[] = [];

  for (const key of visibleSet) {
    const table = tables.get(key);
    if (!table) continue;
    const n = table.numRows;
    total += n;
    const gt = geomTypes.get(key) ?? "unknown";
    const suffix = GEOM_ABBREV[gt];
    parts.push(`${key}: ${n.toLocaleString()} ${suffix}`);
  }

  if (total === 0) {
    setStatus("No layers visible");
    return;
  }

  setStatus(
    `${total.toLocaleString()} features \u2014 ${parts.join(" \u00b7 ")}`
  );
}

function updateActiveLayers() {
  const layers: ActiveLayerInfo[] = [];
  for (const key of tables.keys()) {
    const table = tables.get(key)!;
    const gt = (geomTypes.get(key) ?? "unknown") as GeomType;
    const style = layerStyles.get(key);
    const defaultFill = getDefaultStyle(gt).fillColor;
    const fc = style?.fillColor ?? defaultFill;
    const color = `rgb(${fc[0]},${fc[1]},${fc[2]})`;
    const name = key.includes("/") ? key.split("/").pop()! : key;
    layers.push({
      key,
      name,
      count: table.numRows,
      color,
      visible: visibleSet.has(key),
    });
  }
  renderActiveLayers(layers);
}

// ---------------------------------------------------------------------------
// Click / Identify
// ---------------------------------------------------------------------------

const handleClick: FeatureClickHandler = (info) => {
  if (identifyActive) {
    addIdentifyResult(info);
  } else {
    const canvas = getMap().getCanvas();
    const rect = canvas.getBoundingClientRect();
    showPopup(info, lastMouseX - rect.left, lastMouseY - rect.top);
  }
};

// ---------------------------------------------------------------------------
// Layer loading with viewport bbox
// ---------------------------------------------------------------------------

/** Return the feature limit per layer (uniform across all geometry types).
 *  The real OOM guard is MAX_RESPONSE_BYTES (256 MB) in geoarrow.ts. */
function getEffectiveLimit(_gt?: GeomType, _zoom?: number): number {
  return MAX_FEATURES_PER_LAYER;
}

/** Compute simplification tolerance for a given zoom level.
 *  Returns undefined above zoom 12 (full resolution). */
function getSimplifyTolerance(zoom: number): number | undefined {
  if (zoom >= 12) return undefined;
  return 360 / (Math.pow(2, zoom) * 256);
}

/** Scale earcut cooldown based on polygon count. */
function getEarcutCooldown(numRows: number): number {
  if (numRows < 1_000) return 2_000;
  if (numRows < 10_000) return 5_000;
  if (numRows < 50_000) return 10_000;
  return 20_000;
}

/** Load (or reload) a layer using the current viewport bbox. */
async function loadLayerWithViewport(
  ns: string,
  layer: string
): Promise<void> {
  const key = `${ns}/${layer}`;

  // Skip if this layer is already being loaded (prevents moveend race).
  if (loadingKeys.has(key)) return;
  loadingKeys.add(key);

  const viewportBbox = getViewportBbox();
  // Fetch 1.5x the viewport so small pans don't trigger refetches
  const fetchBbox = expandBbox(viewportBbox, 1.5);
  const knownType = geomTypes.get(key);
  const zoom = getMap().getZoom();

  const tf = timeFilters.get(key);

  setTreeLayerLoading(ns, layer, true);
  try {
    const limit = getEffectiveLimit(knownType, zoom);
    const simplify = knownType === "polygon" ? getSimplifyTolerance(zoom) : undefined;

    const table = await loadLayer(ns, layer, fetchBbox, limit, simplify, undefined, tf);
    const gt = detectGeomType(table);
    tables.set(key, table);
    geomTypes.set(key, gt);
    loadedBbox.set(key, fetchBbox);
    loadedZoom.set(key, zoom);
    updateTreeLayerCount(ns, layer, table.numRows);

    debugLog(`loaded ${key}: ${table.numRows} rows, geomType=${gt}, batches=${table.batches.length}`);
    for (const f of table.schema.fields) {
      const ext = f.metadata.get("ARROW:extension:name") ?? "";
      debugLog(`  field: ${f.name}  type=${f.type}  ext=${ext}`);
    }

    if (gt === "polygon") {
      debugLogGeometry(table, key);
    }

    // For polygon layers, set a dynamic cooldown that scales with feature count
    if (gt === "polygon") {
      const cooldown = getEarcutCooldown(table.numRows);
      earcutCooldownKeys.add(key);
      setTimeout(() => {
        earcutCooldownKeys.delete(key);
        debugLog(`earcut cooldown expired for ${key} (${cooldown}ms)`);
      }, cooldown);
    }
  } catch (e) {
    console.error(`Failed to load ${key}:`, e);
    debugLog(`LOAD ERROR ${key}: ${(e as Error).message}`, "err");
    // If we already had cached data, keep the layer visible with stale data.
    // Only uncheck the layer if this was the first load (no cached data).
    if (!tables.has(key)) {
      visibleSet.delete(key);
      setTreeLayerChecked(ns, layer, false);
    }
    setStatus(`Failed to load ${key}: ${(e as Error).message}`);
  }
  setTreeLayerLoading(ns, layer, false);
  loadingKeys.delete(key);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  setStatus("Initializing...");

  // Restore viewport from URL hash (if present)
  const urlState = readUrlState();

  // Load basemap configuration before map init
  const basemapConfigs = await loadBasemapConfig();
  const map = initMap(basemapConfigs[0].style, urlState.center, urlState.zoom);

  // Discover available namespaces from the catalog (tree endpoint preferred)
  let namespacePaths: string[][];
  try {
    namespacePaths = await fetchNamespaceTree();
  } catch {
    // Fallback: flat namespace list wrapped as single-segment paths
    const flatNs = await fetchNamespaces().catch(() => ["colorado"]);
    namespacePaths = flatNs.map((ns) => [ns]);
  }

  // Discover tables per namespace (in parallel)
  setStatus("Discovering tables...");
  const dottedPaths = namespacePaths.map((p) => p.join("."));
  const tablesPerNs = await Promise.all(
    dottedPaths.map(async (ns) => {
      try {
        const tblNames = await fetchTables(ns);
        return [ns, tblNames] as const;
      } catch {
        return [ns, []] as const;
      }
    })
  );
  const tablesMap = Object.fromEntries(tablesPerNs);

  // Build recursive catalog tree and render in sidebar
  const tree = buildCatalogTree(namespacePaths, tablesMap);
  initCatalogBrowser(tree, handleLayerToggle, handleZoom, handleRefresh, (key, opacity) => {
    const style = layerStyles.get(key) ?? getDefaultStyle(geomTypes.get(key) ?? "unknown");
    style.opacity = opacity;
    layerStyles.set(key, style);
    rebuildLayers();
  }, {
    onOpenAttributeTable: handleOpenAttributeTable,
    onOpenSymbology: handleOpenSymbology,
    onSearch: handleSearch,
    onMeasure: handleMeasure,
    onScreenshot: handleScreenshot,
    onResetMap: handleResetMap,
  });
  setStatus("No layers visible");

  // Initialize active layers legend
  initActiveLayers({
    onToggleVisibility: (key) => {
      const [ns, layer] = splitKey(key);
      if (visibleSet.has(key)) {
        visibleSet.delete(key);
      } else {
        visibleSet.add(key);
      }
      setTreeLayerChecked(ns, layer, visibleSet.has(key));
      rebuildLayers();
      updateStatusBar();
      updateActiveLayers();
    },
    onRemove: (key) => {
      const [ns, layer] = splitKey(key);
      visibleSet.delete(key);
      tables.delete(key);
      geomTypes.delete(key);
      layerStyles.delete(key);
      loadedBbox.delete(key);
      loadedZoom.delete(key);
      setTreeLayerChecked(ns, layer, false);
      rebuildLayers();
      updateStatusBar();
      updateActiveLayers();
    },
    onReorder: (orderedKeys) => {
      userLayerOrder = orderedKeys;
      rebuildLayers();
      updateActiveLayers();
    },
    onOpacityChange: (key, opacity) => {
      const gt = (geomTypes.get(key) ?? "unknown") as GeomType;
      const existing = layerStyles.get(key) ?? getDefaultStyle(gt);
      layerStyles.set(key, { ...existing, opacity });
      rebuildLayers();
    },
    onZoom: (ns, layer) => handleZoom(ns, layer),
    onOpenAttributeTable: (ns, layer) => handleOpenAttributeTable(ns, layer),
    onOpenSymbology: (ns, layer) => handleOpenSymbology(ns, layer),
    onDeleteScratch: async (key) => {
      const [ns] = splitKey(key);
      if (!confirm(`Delete scratch namespace "${ns}" and all its tables from the lakehouse?`)) return;
      try {
        const resp = await fetch(`/api/scratch/${encodeURIComponent(ns)}`, { method: "DELETE" });
        if (!resp.ok) {
          const err = await resp.json();
          setStatus(`Error: ${err.error}`);
          return;
        }
        for (const k of [...tables.keys()]) {
          if (k.startsWith(ns + "/")) {
            visibleSet.delete(k);
            tables.delete(k);
            geomTypes.delete(k);
            layerStyles.delete(k);
            loadedBbox.delete(k);
            loadedZoom.delete(k);
          }
        }
        rebuildLayers();
        updateStatusBar();
        updateActiveLayers();
        setStatus(`Deleted scratch namespace "${ns}"`);
      } catch (e) {
        setStatus(`Error deleting scratch: ${(e as Error).message}`);
      }
    },
    onSaveScratch: async (key) => {
      const [ns, table] = splitKey(key);
      const result = await showSaveDialog(ns, table);
      if (!result) return;
      try {
        const resp = await fetch("/api/scratch/save", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            source_namespace: ns,
            source_table: table,
            target_namespace: result.targetNamespace,
            target_table: result.targetTable,
          }),
        });
        if (!resp.ok) {
          const err = await resp.json();
          setStatus(`Error: ${err.error}`);
          return;
        }
        const data = await resp.json();
        const newKey = `${result.targetNamespace}/${result.targetTable}`;
        await loadLayerWithViewport(result.targetNamespace, result.targetTable);
        visibleSet.add(newKey);
        rebuildLayers();
        updateStatusBar();
        updateActiveLayers();
        setStatus(`Saved ${data.rows} rows to ${data.target}`);
      } catch (e) {
        setStatus(`Error saving: ${(e as Error).message}`);
      }
    },
  });

  // Initialize catalog modal
  initCatalogModal();

  // Restore layers from URL hash
  if (urlState.layers && urlState.layers.length > 0) {
    const loadPromises = urlState.layers.map(async (key) => {
      const [ns, layer] = splitKey(key);
      if (!ns || !layer) return;
      visibleSet.add(key);
      setTreeLayerChecked(ns, layer, true);
      await loadLayerWithViewport(ns, layer);
    });
    Promise.all(loadPromises).then(() => {
      rebuildLayers();
      updateStatusBar();
    });
  }

  // -----------------------------------------------------------------------
  // Attribute table handler
  // -----------------------------------------------------------------------

  function handleOpenAttributeTable(ns: string, layer: string) {
    const key = `${ns}/${layer}`;
    const table = tables.get(key);
    if (!table) {
      setStatus(`Load ${key} first to view its attribute table`);
      return;
    }
    if (activeAttrTable) activeAttrTable.destroy();
    activeAttrTable = new AttributeTable(table, key, {
      onClose: () => { activeAttrTable = null; },
    });
    activeAttrTable.mount(document.getElementById("map-container")!);
  }

  // -----------------------------------------------------------------------
  // Symbology handler
  // -----------------------------------------------------------------------

  function handleOpenSymbology(ns: string, layer: string) {
    const key = `${ns}/${layer}`;
    const gt = geomTypes.get(key) ?? "unknown";
    const style = layerStyles.get(key) ?? getDefaultStyle(gt);
    layerStyles.set(key, style);

    if (activeSymPanel) activeSymPanel.destroy();
    activeSymPanel = new SymbologyPanel(
      key,
      gt,
      style,
      (updated) => {
        layerStyles.set(key, updated);
        rebuildLayers();
      },
      () => { activeSymPanel = null; }
    );
    activeSymPanel.mount(document.getElementById("map-container")!);
  }

  // -----------------------------------------------------------------------
  // Search handler (coordinate parse + Nominatim geocode)
  // -----------------------------------------------------------------------

  async function handleSearch(query: string) {
    if (!query) return;

    // Try parsing as coordinates first
    const coords = parseCoordinates(query);
    if (coords) {
      hideSearchResults();
      getMap().flyTo({ center: coords, zoom: 14, duration: 1500 });
      return;
    }

    // Geocode via Nominatim
    try {
      const resp = await fetch(
        `https://nominatim.openstreetmap.org/search?format=json&limit=5&q=${encodeURIComponent(query)}`,
        { headers: { "User-Agent": "LakehouseAI-Webmap/1.0" } }
      );
      if (!resp.ok) return;
      const data = await resp.json();
      if (data.length === 0) {
        setStatus(`No results for "${query}"`);
        return;
      }

      const results: SearchResult[] = data.map((r: any) => ({
        name: r.display_name,
        lat: parseFloat(r.lat),
        lon: parseFloat(r.lon),
        bbox: r.boundingbox
          ? [
              parseFloat(r.boundingbox[2]),
              parseFloat(r.boundingbox[0]),
              parseFloat(r.boundingbox[3]),
              parseFloat(r.boundingbox[1]),
            ] as [number, number, number, number]
          : undefined,
      }));

      showSearchResults(results, (result) => {
        if (result.bbox) {
          flyToBounds(result.bbox);
        } else {
          getMap().flyTo({ center: [result.lon, result.lat], zoom: 14, duration: 1500 });
        }
      });
    } catch (e) {
      console.error("Geocode failed:", e);
    }
  }

  function parseCoordinates(q: string): [number, number] | null {
    // Try "lat, lon" or "lat lon"
    const parts = q.split(/[,\s]+/).map(Number).filter(Number.isFinite);
    if (parts.length === 2) {
      let [a, b] = parts;
      // Heuristic: if |a| > 90, it's likely longitude
      if (Math.abs(a) > 90 && Math.abs(b) <= 90) {
        return [a, b]; // [lng, lat]
      }
      return [b, a]; // [lng, lat] from "lat, lon"
    }
    return null;
  }

  // -----------------------------------------------------------------------
  // Measure handler
  // -----------------------------------------------------------------------

  const measureResult = document.createElement("div");
  measureResult.className = "measure-result";
  document.getElementById("map-container")!.appendChild(measureResult);

  measureTool = new MeasureTool(map, (text) => {
    measureResult.textContent = text;
  });

  function handleMeasure(mode: "distance" | "area" | "none") {
    if (!measureTool) return;
    if (mode === "none") {
      measureTool.deactivate();
      measureResult.textContent = "";
    } else {
      measureTool.activate(mode);
    }
  }

  // -----------------------------------------------------------------------
  // Screenshot handler
  // -----------------------------------------------------------------------

  function handleScreenshot() {
    captureMap(map.getCanvas());
  }

  // -----------------------------------------------------------------------
  // Reset map — remove all layers and return to initial view
  // -----------------------------------------------------------------------

  function handleResetMap() {
    debugLog("Reset map triggered");
    // Uncheck all catalog tree checkboxes
    for (const key of [...tables.keys()]) {
      const [ns, layer] = splitKey(key);
      setTreeLayerChecked(ns, layer, false);
    }
    // Destroy any active time sliders
    for (const slider of activeTimeSliders.values()) slider.destroy();
    activeTimeSliders.clear();
    timeFilters.clear();
    // Clear all layer state
    tables.clear();
    visibleSet.clear();
    geomTypes.clear();
    layerStyles.clear();
    loadedBbox.clear();
    loadedZoom.clear();
    userLayerOrder = null;
    // Rebuild (clears all deck.gl layers)
    rebuildLayers();
    updateActiveLayers();
    updateStatusBar();
    // Fly back to initial view
    resetView();
  }

  // -----------------------------------------------------------------------
  // Time slider — check for temporal columns on first layer load
  // -----------------------------------------------------------------------

  async function checkTemporalColumns(ns: string, layer: string) {
    const key = `${ns}/${layer}`;
    if (activeTimeSliders.has(key)) return;
    try {
      const schema = await fetchSchema(ns, layer);
      if (schema.temporal_columns.length === 0) return;

      const tc = schema.temporal_columns[0]; // Use first temporal column
      // Parse min/max as UTC to avoid local-timezone shift.
      // The server returns "YYYY-MM-DD HH:MM:SS" without timezone info;
      // appending "Z" forces UTC interpretation so toISOString() round-trips
      // correctly back to the same wall-clock time the database stores.
      const config: TimeConfig = {
        column: tc.name,
        min: new Date(tc.min.replace(" ", "T") + "Z"),
        max: new Date(tc.max.replace(" ", "T") + "Z"),
        distinctCount: tc.distinct_count,
      };

      /** Format a Date for the DuckDB time filter query (no timezone, no T). */
      const fmtForQuery = (d: Date): string =>
        d.toISOString().slice(0, 19).replace("T", " ");

      const slider = new TimeSlider(config, async (start, end) => {
        timeFilters.set(key, {
          column: tc.name,
          start: fmtForQuery(start),
          end: fmtForQuery(end),
        });
        // Force reload with the new time filter
        loadedBbox.delete(key);
        await loadLayerWithViewport(ns, layer);
        rebuildLayers();
        updateStatusBar();
      }, () => {
        // On close — remove time filter and reload
        timeFilters.delete(key);
        activeTimeSliders.delete(key);
        loadedBbox.delete(key);
        loadLayerWithViewport(ns, layer).then(() => {
          rebuildLayers();
          updateStatusBar();
        });
      });

      slider.mount(document.getElementById("map-container")!);
      activeTimeSliders.set(key, slider);
    } catch {
      // Schema endpoint not available — skip time slider
    }
  }

  // --- Sidebar toggle ---
  const sidebar = document.getElementById("sidebar");
  const sidebarToggle = document.getElementById("sidebar-toggle");
  if (sidebar && sidebarToggle) {
    sidebarToggle.addEventListener("click", () => {
      const collapsed = sidebar.classList.toggle("collapsed");
      sidebarToggle.classList.toggle("collapsed", collapsed);
      sidebarToggle.innerHTML = collapsed ? "&#x276F;" : "&#x276E;";
      sidebarToggle.title = collapsed ? "Expand sidebar" : "Collapse sidebar";
      // Let the CSS transition finish, then tell MapLibre the container changed
      setTimeout(() => map.resize(), 260);
    });
  }

  // --- Mouse tracking for popups ---
  map.getCanvas().addEventListener("mousemove", (e) => {
    lastMouseX = e.clientX;
    lastMouseY = e.clientY;
  });

  map.on("click", () => {
    if (!identifyActive) hidePopup();
  });

  // -----------------------------------------------------------------------
  // Viewport-aware reload on pan/zoom (debounced)
  // -----------------------------------------------------------------------

  let reloadTimer: ReturnType<typeof setTimeout> | null = null;

  onMoveEnd(() => {
    // Update URL hash with current viewport + visible layers
    const c = getMap().getCenter();
    writeUrlState({
      zoom: getMap().getZoom(),
      center: [c.lng, c.lat],
      layers: [...visibleSet],
    });

    if (visibleSet.size === 0) return;
    if (reloadTimer) clearTimeout(reloadTimer);
    reloadTimer = setTimeout(reloadVisibleLayers, 400);
  });

  async function reloadVisibleLayers() {
    const bbox = getViewportBbox();

    const reloadKeys = [...visibleSet].filter((key) => {
      if (loadingKeys.has(key)) return false;
      if (earcutCooldownKeys.has(key)) return false;

      const prev = loadedBbox.get(key);
      if (!prev) return true;
      return !bboxContains(prev, bbox);
    });
    if (reloadKeys.length === 0) return;

    await Promise.all(
      reloadKeys.map((key) => {
        const [ns, layer] = splitKey(key);
        return loadLayerWithViewport(ns, layer);
      })
    );
    rebuildLayers();
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Layer toggle handler (called from tree UI)
  // -----------------------------------------------------------------------

  async function handleLayerToggle(
    ns: string,
    layer: string,
    visible: boolean
  ) {
    const key = `${ns}/${layer}`;

    if (visible) {
      visibleSet.add(key);

      // Load on demand
      if (!tables.has(key)) {
        await loadLayerWithViewport(ns, layer);
        // Check for temporal columns on first load
        checkTemporalColumns(ns, layer);
      }
    } else {
      visibleSet.delete(key);
      loadedZoom.delete(key);
    }

    rebuildLayers();
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Refresh handler
  // -----------------------------------------------------------------------

  async function handleRefresh() {
    debugLog("Manual refresh triggered");
    // Clear bbox + cooldown caches but keep table data so a
    // failed reload doesn't lose the layer.
    for (const key of [...visibleSet]) {
      loadedBbox.delete(key);
      loadedZoom.delete(key);
      earcutCooldownKeys.delete(key);
    }
    setStatus("Refreshing...");
    await Promise.all(
      [...visibleSet].map((key) => {
        const [ns, layer] = splitKey(key);
        return loadLayerWithViewport(ns, layer);
      })
    );
    rebuildLayers();
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Zoom-to-extent handler (called from tree zoom button)
  // -----------------------------------------------------------------------

  async function handleZoom(ns: string, layer: string) {
    setStatus(`Fetching extent for ${ns}/${layer}...`);
    try {
      const bbox = await fetchTableBbox(ns, layer);
      flyToBounds(bbox);

      // If the layer isn't visible yet, turn it on and load it
      const key = `${ns}/${layer}`;
      if (!visibleSet.has(key)) {
        visibleSet.add(key);
        setTreeLayerChecked(ns, layer, true);

        // Wait a moment for the fly animation to start, then load with the
        // new viewport (the moveend handler will also reload)
        setTimeout(async () => {
          await loadLayerWithViewport(ns, layer);
          rebuildLayers();
          updateStatusBar();
        }, 600);
      }
    } catch (e) {
      console.error(`Failed to get bbox for ${ns}/${layer}:`, e);
    }
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Identify mode
  // -----------------------------------------------------------------------

  const mapEl = document.getElementById("map")!;
  const selectBox = document.getElementById("select-box")!;
  let dragStart: { x: number; y: number } | null = null;

  function processBoxSelection(
    x: number,
    y: number,
    width: number,
    height: number
  ) {
    const results = pickObjectsInRect(x, y, width, height);
    for (const info of results) {
      if (info.index < 0 || !info.layer) continue;
      const layerId = (info.layer.id as string) ?? "";
      const table = tables.get(layerId);
      if (!table) continue;
      const props = getFeatureProps(table, info.index, layerId);
      addIdentifyResult(props);
    }
  }

  map.getCanvas().addEventListener("pointerdown", (e) => {
    if (!identifyActive) return;
    const rect = map.getCanvas().getBoundingClientRect();
    dragStart = { x: e.clientX - rect.left, y: e.clientY - rect.top };
  });

  window.addEventListener("pointermove", (e) => {
    if (!dragStart || !identifyActive) return;
    const rect = map.getCanvas().getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    const dx = Math.abs(cx - dragStart.x);
    const dy = Math.abs(cy - dragStart.y);
    if (dx + dy > 5) {
      map.dragPan.disable();
      selectBox.style.display = "block";
      selectBox.style.left = `${Math.min(dragStart.x, cx)}px`;
      selectBox.style.top = `${Math.min(dragStart.y, cy)}px`;
      selectBox.style.width = `${dx}px`;
      selectBox.style.height = `${dy}px`;
    }
  });

  window.addEventListener("pointerup", (e) => {
    if (!dragStart || !identifyActive) return;
    const rect = map.getCanvas().getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    const dx = Math.abs(cx - dragStart.x);
    const dy = Math.abs(cy - dragStart.y);

    selectBox.style.display = "none";
    const wasDrag = dx + dy > 5;
    const start = dragStart;
    dragStart = null;
    map.dragPan.enable();

    if (wasDrag) {
      const bx = Math.min(start.x, cx);
      const by = Math.min(start.y, cy);
      processBoxSelection(bx, by, dx, dy);
    }
  });

  initIdentifyToggle((active) => {
    identifyActive = active;
    if (active) {
      hidePopup();
      mapEl.classList.add("identify-cursor");
      showIdentifyPanel(() => clearIdentifyResults());
    } else {
      mapEl.classList.remove("identify-cursor");
      hideIdentifyPanel();
      dragStart = null;
      selectBox.style.display = "none";
    }
  });

  // Basemap picker (only shows when multiple basemaps configured)
  // After basemap switch, rebuild layers so deck.gl creates fresh WebGL
  // resources for the new GL context (MapLibre destroys the old one on
  // setStyle).
  initBasemapPicker(getBasemaps(), getCurrentBasemapIndex(), async (index) => {
    await switchBasemap(index);
    rebuildLayers();
    updateStatusBar();
  });
}

// ---------------------------------------------------------------------------
// Diagnostics — geometry data inspection
// ---------------------------------------------------------------------------

/** Log raw coordinate values from the Arrow table to verify data integrity */
function debugLogGeometry(table: Table, key: string): void {
  try {
    const geomCol = table.getChild("geometry") as Vector | null;
    if (!geomCol || geomCol.data.length === 0) {
      debugLog(`${key}: no geometry column or empty data`, "warn");
      return;
    }

    const batch = geomCol.data[0];
    debugLog(`${key}: geom batch0: length=${batch.length}, type=${batch.type}, children=${batch.children.length}`);

    // Navigate nested List<List<FixedSizeList[2]<Float64>>> to raw coordinates
    let d = batch;
    let depth = 0;
    const typePath: string[] = [String(d.type)];
    while (d.children?.length > 0 && depth < 6) {
      d = d.children[0];
      depth++;
      typePath.push(String(d.type));
    }
    debugLog(`${key}: nested path (depth=${depth}): ${typePath.join(" → ")}`);

    // Log offsets at each level for the first few polygons
    const b0 = batch;
    if (b0.valueOffsets) {
      const offs = Array.from(b0.valueOffsets.slice(0, 6));
      debugLog(`${key}: polygon offsets[0..5]: ${offs.join(", ")}`);
    }
    if (b0.children[0]?.valueOffsets) {
      const offs = Array.from(b0.children[0].valueOffsets.slice(0, 10));
      debugLog(`${key}: ring offsets[0..9]: ${offs.join(", ")}`);
    }

    // Extract raw Float64 coordinate values
    if (d.values instanceof Float64Array) {
      const v = d.values;
      debugLog(`${key}: raw Float64Array length=${v.length}`);
      const n = Math.min(20, v.length);
      const sample = Array.from(v.slice(0, n)).map((x) => x.toFixed(6));
      debugLog(`${key}: first ${n} coord values: [${sample.join(", ")}]`);
    } else {
      debugLog(`${key}: no Float64Array found at depth ${depth}`, "warn");
    }
  } catch (e) {
    debugLog(`${key}: geometry debug error: ${(e as Error).message}`, "err");
  }
}

/** Check the deck overlay for sub-layer state (diagnostic) */
function debugCheckDeckLayers(key: string): void {
  setTimeout(() => {
    try {
      const overlay = getOverlay() as any;
      if (!overlay) return;
      const deck = overlay._deck;
      if (!deck) return;
      const allLayers = deck.props?.layers ?? [];
      debugLog(`${key}: deck.props.layers count=${allLayers.length}`);
      const layerManager = deck.layerManager;
      if (layerManager) {
        const layers = layerManager.getLayers();
        debugLog(`${key}: layerManager has ${layers.length} total layers`);
      }
    } catch (e) {
      debugLog(`${key}: deck inspection error: ${(e as Error).message}`, "err");
    }
  }, 2000);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function splitKey(key: string): [string, string] {
  const slash = key.indexOf("/");
  return [key.slice(0, slash), key.slice(slash + 1)];
}

/** Check if bounding box `outer` fully contains `inner`. */
function bboxContains(outer: Bbox, inner: Bbox): boolean {
  return (
    outer[0] <= inner[0] &&
    outer[1] <= inner[1] &&
    outer[2] >= inner[2] &&
    outer[3] >= inner[3]
  );
}

// Catch unhandled promise rejections for diagnostics.
window.addEventListener("unhandledrejection", (event) => {
  const msg =
    event.reason instanceof Error
      ? event.reason.message
      : String(event.reason);
  const stack =
    event.reason instanceof Error ? event.reason.stack ?? "" : "";
  debugLog(`UNHANDLED REJECTION: ${msg}`, "err");
  if (stack) debugLog(`  stack: ${stack.split("\n").slice(0, 3).join(" | ")}`, "err");
  console.error("[webmap] Unhandled rejection:", event.reason);
  setStatus(`⚠ ${msg}`);
});

window.addEventListener("error", (event) => {
  debugLog(`GLOBAL ERROR: ${event.message} at ${event.filename}:${event.lineno}`, "err");
});

main().catch((err) => {
  console.error("Webmap initialization failed:", err);
  setStatus(`Error: ${err.message}`);
});

// ---------------------------------------------------------------------------
// Agent Integration
// ---------------------------------------------------------------------------

import("./agent-ws").then(({ AgentWebSocket }) => {
  import("./chat-panel").then(({ ChatPanel }) => {
    const sessionId = crypto.randomUUID();
    const panel = new ChatPanel(sessionId);
    let wsClient: InstanceType<typeof AgentWebSocket> | null = null;
    let panelOpen = false;

    // Provide active layer keys to the chat panel for namespace filtering
    panel.setActiveLayersProvider(() => [...visibleSet]);

    // Clear button: drop scratch namespace, remove layers, clear chat
    const scratchNs = `_scratch_${sessionId.replace(/-/g, "").slice(0, 8)}`;
    panel.setClearHandler(async () => {
      try {
        await fetch(`/api/scratch/${scratchNs}`, { method: "DELETE" });
      } catch {
        /* best effort */
      }
      // Remove all layers from this scratch namespace
      for (const key of [...tables.keys()]) {
        const [ns] = splitKey(key);
        if (ns === scratchNs) {
          tables.delete(key);
          geomTypes.delete(key);
          visibleSet.delete(key);
          loadedBbox.delete(key);
        }
      }
      rebuildLayers();
      updateStatusBar();
      panel.clearMessages();
    });

    // Debounced layer_ready batching — collect rapid-fire events
    type LREvent = import("./agent-ws").LayerReadyEvent;
    let pendingEvents: LREvent[] = [];
    let debounceTimer: ReturnType<typeof setTimeout> | null = null;

    function queueLayerReady(event: LREvent) {
      pendingEvents.push(event);
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(flushPendingLayers, 300);
    }

    async function flushPendingLayers() {
      const batch = pendingEvents.splice(0);
      debounceTimer = null;
      if (batch.length === 0) return;

      // Load all queued layers in parallel
      const results = await Promise.allSettled(
        batch.map(async (event) => {
          const key = `${event.namespace}/${event.table}`;
          const arrowTable = await loadLayer(event.namespace, event.table);
          if (!arrowTable) return null;
          const geomType = detectGeomType(arrowTable);
          if (!geomType) return null;

          tables.set(key, arrowTable);
          geomTypes.set(key, geomType);
          visibleSet.add(key);
          updateTreeLayerCount(event.namespace, event.table, event.row_count);
          return event;
        }),
      );

      rebuildLayers();
      updateStatusBar();

      // Fly to union bbox of all successful loads
      const bboxes = results
        .filter(
          (r): r is PromiseFulfilledResult<LREvent | null> =>
            r.status === "fulfilled" && r.value?.bbox != null,
        )
        .map((r) => r.value!.bbox!);
      if (bboxes.length > 0) {
        const union: [number, number, number, number] = [
          Math.min(...bboxes.map((b) => b[0])),
          Math.min(...bboxes.map((b) => b[1])),
          Math.max(...bboxes.map((b) => b[2])),
          Math.max(...bboxes.map((b) => b[3])),
        ];
        flyToBounds(union);
      }
    }

    // Use the sidebar toggle button (placed in index.html)
    const agentSection = document.getElementById("agent-section");
    const toggleBtn = document.getElementById("agent-toggle-btn");
    if (agentSection) agentSection.style.display = "";
    const descEl = toggleBtn?.querySelector(".sidebar-link-desc");

    const chatSidebar = document.getElementById("chat-sidebar")!;

    toggleBtn?.addEventListener("click", () => {
      panelOpen = !panelOpen;
      if (panelOpen) {
        panel.mount(chatSidebar);
        setTimeout(() => getMap().resize(), 260);
        wsClient = new AgentWebSocket(
          sessionId,
          queueLayerReady,
          (connected) => panel.setAgentStatus(connected),
        );
        wsClient.connect();
        if (descEl) descEl.textContent = "Click to close";
      } else {
        panel.unmount();
        setTimeout(() => getMap().resize(), 260);
        wsClient?.disconnect();
        wsClient = null;
        pendingEvents = [];
        if (debounceTimer) {
          clearTimeout(debounceTimer);
          debounceTimer = null;
        }
        if (descEl) descEl.textContent = "Natural language queries";
      }
    });
  });
});
