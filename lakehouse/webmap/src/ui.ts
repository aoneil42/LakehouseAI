export type LayerToggleCallback = (
  ns: string,
  layer: string,
  visible: boolean
) => void;

export type LayerZoomCallback = (ns: string, layer: string) => void;

export type RefreshCallback = () => void;

export type OpacityChangeCallback = (key: string, opacity: number) => void;

export type OpenAttributeTableCallback = (ns: string, layer: string) => void;
export type OpenSymbologyCallback = (ns: string, layer: string) => void;
export type SearchCallback = (query: string) => void;
export type MeasureCallback = (mode: "distance" | "area" | "none") => void;
export type ScreenshotCallback = () => void;
export type ResetMapCallback = () => void;

/** Per-namespace table lists: { colorado: ["points","lines","polygons"] } */
export type NsTableMap = Record<string, string[]>;

// Consistent colour palette for layer dots — cycles for arbitrary table names
const DOT_PALETTE = [
  "#1e90ff",
  "#dc143c",
  "#4169e1",
  "#ff8c00",
  "#2e8b57",
  "#9370db",
  "#20b2aa",
  "#cd5c5c",
];

function dotColor(index: number): string {
  return DOT_PALETTE[index % DOT_PALETTE.length];
}

function titleCase(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

// ---------------------------------------------------------------------------
// Catalog Tree Data Model
// ---------------------------------------------------------------------------

export interface CatalogNode {
  name: string; // display name (last segment)
  fullPath: string; // dotted full path, e.g. "colorado.water"
  type: "namespace" | "table";
  children: CatalogNode[];
}

/**
 * Build a recursive catalog tree from flat namespace paths + table lists.
 *
 * @param namespacePaths  e.g. [["colorado"], ["colorado", "water"]]
 * @param tablesPerNs     dotted path → table names
 */
export function buildCatalogTree(
  namespacePaths: string[][],
  tablesPerNs: Record<string, string[]>
): CatalogNode[] {
  const root: CatalogNode[] = [];

  // Sort so parents come before children
  const sorted = [...namespacePaths].sort(
    (a, b) => a.length - b.length || a.join(".").localeCompare(b.join("."))
  );

  for (const parts of sorted) {
    let level = root;
    for (let i = 0; i < parts.length; i++) {
      const segment = parts[i];
      const fullPath = parts.slice(0, i + 1).join(".");
      let existing = level.find(
        (n) => n.fullPath === fullPath && n.type === "namespace"
      );
      if (!existing) {
        existing = {
          name: segment,
          fullPath,
          type: "namespace",
          children: [],
        };
        level.push(existing);
      }
      level = existing.children;
    }
  }

  // Add table leaf nodes under each namespace
  function addTables(nodes: CatalogNode[]): void {
    for (const node of nodes) {
      if (node.type !== "namespace") continue;
      const tables = tablesPerNs[node.fullPath] ?? [];
      for (const tableName of tables) {
        node.children.push({
          name: tableName,
          fullPath: `${node.fullPath}/${tableName}`,
          type: "table",
          children: [],
        });
      }
      addTables(node.children.filter((c) => c.type === "namespace"));
    }
  }
  addTables(root);

  return root;
}

// ---------------------------------------------------------------------------
// Layer Tree (legacy — flat namespace list, renders in #controls)
// ---------------------------------------------------------------------------

export function initLayerTree(
  nsTables: NsTableMap,
  onToggle: LayerToggleCallback,
  onZoom: LayerZoomCallback,
  onRefresh?: RefreshCallback,
): void {
  const container = document.getElementById("controls")!;
  const namespaces = Object.keys(nsTables);

  const nsHtml = namespaces
    .map((ns) => {
      const tables = nsTables[ns] ?? [];
      return `
        <div class="tree-ns" data-ns="${ns}">
          <div class="tree-ns-header">
            <span class="tree-caret collapsed" data-caret="${ns}">\u25B8</span>
            <label class="tree-ns-check">
              <input type="checkbox" data-ns-check="${ns}" />
              <span class="tree-ns-name">${ns}</span>
            </label>
          </div>
          <div class="tree-ns-layers collapsed" data-ns-layers="${ns}">
            ${tables
              .map(
                (layer, i) => `
              <div class="layer-row">
                <label class="layer-toggle">
                  <input type="checkbox" data-ns="${ns}" data-layer="${layer}" />
                  <span class="layer-dot" style="background:${dotColor(i)}"></span>
                  <span class="layer-label">${titleCase(layer)}</span>
                  <span class="layer-count" data-lcount="${ns}/${layer}">\u2014</span>
                </label>
              </div>`
              )
              .join("")}
          </div>
        </div>`;
    })
    .join("");

  container.innerHTML = `
    <div class="controls-inner">
      <div class="controls-title">Layers</div>
      ${nsHtml}
      <div class="toolbar-section">
        <div class="toolbar-row">
          <button id="refresh-btn" class="toolbar-btn" title="Reload visible layers">&#8635; Refresh</button>
        </div>
      </div>
    </div>
  `;

  wireLayerTreeEvents(container, onToggle, onZoom);

  document.getElementById("refresh-btn")?.addEventListener("click", () => {
    onRefresh?.();
  });
}

// ---------------------------------------------------------------------------
// Catalog Browser (nested namespace tree, renders in #sidebar)
// ---------------------------------------------------------------------------

let globalColorIndex = 0;

function renderTreeNodes(nodes: CatalogNode[], depth: number): string {
  return nodes
    .map((node) => {
      if (node.type === "namespace") {
        const collapsed = depth > 0 ? " collapsed" : "";
        const caretChar = depth > 0 ? "\u25B8" : "\u25BE";
        return `
        <div class="cat-node cat-ns" data-ns="${node.fullPath}">
          <div class="cat-node-header" style="padding-left:${depth * 16 + 4}px">
            <span class="tree-caret${collapsed}" data-caret="${node.fullPath}">${caretChar}</span>
            <span class="cat-folder-icon">\uD83D\uDCC1</span>
            <label class="cat-ns-check">
              <input type="checkbox" data-ns-check="${node.fullPath}" />
              <span class="cat-ns-name">${node.name}</span>
            </label>
          </div>
          <div class="cat-ns-children${collapsed}" data-ns-children="${node.fullPath}">
            ${renderTreeNodes(node.children, depth + 1)}
          </div>
        </div>`;
      } else {
        // Table leaf node — ns is everything before the last /
        const slashIdx = node.fullPath.lastIndexOf("/");
        const ns = node.fullPath.slice(0, slashIdx);
        const table = node.fullPath.slice(slashIdx + 1);
        const color = dotColor(globalColorIndex++);
        return `
        <div class="cat-node cat-table" style="padding-left:${depth * 16 + 4}px">
          <label class="layer-toggle">
            <input type="checkbox" data-ns="${ns}" data-layer="${table}" />
            <span class="cat-table-icon">\uD83D\uDCC4</span>
            <span class="layer-dot" style="background:${color}"></span>
            <span class="layer-label">${titleCase(table)}</span>
            <span class="layer-count" data-lcount="${ns}/${table}">\u2014</span>
          </label>
          <input type="range" class="opacity-slider" data-opacity-key="${ns}/${table}" min="0" max="100" value="100" title="Opacity" />
        </div>`;
      }
    })
    .join("");
}

export interface CatalogBrowserCallbacks {
  onToggle: LayerToggleCallback;
  onZoom: LayerZoomCallback;
  onRefresh?: RefreshCallback;
  onOpacityChange?: OpacityChangeCallback;
  onOpenAttributeTable?: OpenAttributeTableCallback;
  onOpenSymbology?: OpenSymbologyCallback;
  onSearch?: SearchCallback;
  onMeasure?: MeasureCallback;
  onScreenshot?: ScreenshotCallback;
  onResetMap?: ResetMapCallback;
}

export function initCatalogBrowser(
  tree: CatalogNode[],
  onToggle: LayerToggleCallback,
  onZoom: LayerZoomCallback,
  onRefresh?: RefreshCallback,
  onOpacityChange?: OpacityChangeCallback,
  callbacks?: Partial<CatalogBrowserCallbacks>,
): void {
  globalColorIndex = 0;

  const sidebarNav = document.querySelector(".sidebar-nav")!;

  // Merge legacy positional params with callbacks object
  const cbs: CatalogBrowserCallbacks = {
    onToggle,
    onZoom,
    onRefresh,
    onOpacityChange,
    ...callbacks,
  };

  // ── Sidebar section: Layers legend + tools ──────────────────────────
  const browserSection = document.createElement("div");
  browserSection.id = "catalog-browser";
  browserSection.className = "sidebar-section";
  browserSection.innerHTML = `
    <h2 class="sidebar-section-title">Layers</h2>
    <div class="search-container" style="position:relative;">
      <input type="text" class="search-input" placeholder="Search coordinates or places..." />
      <div class="search-results" style="display:none;"></div>
    </div>
    <div id="sidebar-layers"></div>
    <button id="add-data-btn" class="sidebar-add-data-btn">+ Add Data</button>
    <div class="catalog-toolbar">
      <div class="toolbar-row">
        <label class="toolbar-label">Basemap</label>
        <select id="basemap-select" class="toolbar-select"></select>
      </div>
      <div class="toolbar-row toolbar-row-btns">
        <button id="refresh-btn" class="toolbar-btn" title="Reload visible layers">&#8635; Refresh</button>
        <button id="reset-map-btn" class="toolbar-btn toolbar-btn-danger" title="Remove all layers and reset view">&#x21BA; Reset</button>
      </div>
      <div class="tools-section">
        <div class="tools-row">
          <button class="tool-btn" id="measure-dist-btn" title="Measure distance (geodesic)">Dist</button>
          <button class="tool-btn" id="measure-area-btn" title="Measure area (geodesic)">Area</button>
        </div>
      </div>
    </div>
  `;
  sidebarNav.insertBefore(browserSection, sidebarNav.firstChild);

  // ── Catalog modal: tree browser ─────────────────────────────────────
  const modalBody = document.getElementById("catalog-modal-tree");
  if (modalBody) {
    modalBody.innerHTML = `
      <div class="catalog-modal-search">
        <input type="text" class="catalog-filter-input" placeholder="Filter layers..." />
      </div>
      <div id="catalog-tree" class="catalog-tree">${renderTreeNodes(tree, 0)}</div>
    `;
    const treeContainer = document.getElementById("catalog-tree")!;
    wireLayerTreeEvents(treeContainer, cbs.onToggle, cbs.onZoom);
    // Context menu is on the sidebar legend only, not the Add Data modal

    // Opacity slider events in catalog tree
    if (cbs.onOpacityChange) {
      treeContainer.addEventListener("input", (e) => {
        const target = e.target as HTMLInputElement;
        if (!target.classList.contains("opacity-slider")) return;
        const key = target.dataset.opacityKey;
        if (key) cbs.onOpacityChange!(key, parseInt(target.value, 10) / 100);
      });
    }

    // Filter input for catalog tree
    const filterInput = modalBody.querySelector(".catalog-filter-input") as HTMLInputElement;
    if (filterInput) {
      let filterTimer: ReturnType<typeof setTimeout>;
      filterInput.addEventListener("input", () => {
        clearTimeout(filterTimer);
        filterTimer = setTimeout(() => {
          const q = filterInput.value.toLowerCase().trim();
          treeContainer.querySelectorAll<HTMLElement>(".cat-table").forEach((node) => {
            const label = node.querySelector(".layer-label")?.textContent?.toLowerCase() ?? "";
            node.style.display = label.includes(q) ? "" : "none";
          });
          // Show/hide namespaces that have visible children
          treeContainer.querySelectorAll<HTMLElement>(".cat-ns").forEach((nsNode) => {
            const hasVisible = nsNode.querySelector('.cat-table:not([style*="display: none"])');
            nsNode.style.display = hasVisible || !q ? "" : "none";
          });
        }, 200);
      });
    }
  }

  // "Add Data" button opens the catalog modal
  document.getElementById("add-data-btn")?.addEventListener("click", () => {
    openCatalogModal();
  });

  document.getElementById("refresh-btn")?.addEventListener("click", () => {
    cbs.onRefresh?.();
  });

  document.getElementById("reset-map-btn")?.addEventListener("click", () => {
    cbs.onResetMap?.();
  });

  // Search box (coordinate/geocode search, stays in sidebar)
  if (cbs.onSearch) {
    const searchInput = browserSection.querySelector(".search-input") as HTMLInputElement;
    let searchTimer: ReturnType<typeof setTimeout>;
    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        clearTimeout(searchTimer);
        cbs.onSearch!(searchInput.value.trim());
      }
    });
    searchInput.addEventListener("input", () => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => {
        const q = searchInput.value.trim();
        if (q.length >= 3) cbs.onSearch!(q);
      }, 600);
    });
  }

  // Measure buttons
  if (cbs.onMeasure) {
    const distBtn = document.getElementById("measure-dist-btn")!;
    const areaBtn = document.getElementById("measure-area-btn")!;
    distBtn.addEventListener("click", () => {
      const isActive = distBtn.classList.toggle("active");
      areaBtn.classList.remove("active");
      cbs.onMeasure!(isActive ? "distance" : "none");
    });
    areaBtn.addEventListener("click", () => {
      const isActive = areaBtn.classList.toggle("active");
      distBtn.classList.remove("active");
      cbs.onMeasure!(isActive ? "area" : "none");
    });
  }

}

// ---------------------------------------------------------------------------
// Shared event wiring (used by both initLayerTree and initCatalogBrowser)
// ---------------------------------------------------------------------------

function wireLayerTreeEvents(
  container: HTMLElement,
  onToggle: LayerToggleCallback,
  onZoom: LayerZoomCallback
): void {
  // Checkbox changes (layer + namespace-level)
  container.addEventListener("change", (e) => {
    const input = e.target as HTMLInputElement;
    const ns = input.dataset.ns;
    const layer = input.dataset.layer;
    const nsCheck = input.dataset.nsCheck;

    if (ns && layer) {
      onToggle(ns, layer, input.checked);
      syncNsCheckbox(ns);
    } else if (nsCheck) {
      const checked = input.checked;
      // Toggle all descendant table checkboxes (exact match + nested)
      const directInputs = container.querySelectorAll<HTMLInputElement>(
        `input[data-ns="${nsCheck}"][data-layer]`
      );
      const nestedInputs = container.querySelectorAll<HTMLInputElement>(
        `input[data-ns^="${nsCheck}."][data-layer]`
      );
      for (const li of [...directInputs, ...nestedInputs]) {
        if (li.checked !== checked) {
          li.checked = checked;
          onToggle(li.dataset.ns!, li.dataset.layer!, checked);
        }
      }
      // Also sync child namespace checkboxes
      const childNsInputs = container.querySelectorAll<HTMLInputElement>(
        `input[data-ns-check^="${nsCheck}."]`
      );
      for (const ci of childNsInputs) {
        ci.checked = checked;
        ci.indeterminate = false;
      }
      if (checked) expandNamespace(nsCheck);
    }
  });

  // Caret click to toggle namespace collapse
  container.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;

    const caret = target.closest("[data-caret]") as HTMLElement | null;
    if (caret) {
      const ns = caret.dataset.caret!;
      const children =
        document.querySelector(`[data-ns-children="${ns}"]`) ??
        document.querySelector(`[data-ns-layers="${ns}"]`);
      if (children) {
        const isCollapsed = children.classList.toggle("collapsed");
        caret.classList.toggle("collapsed", isCollapsed);
        caret.textContent = isCollapsed ? "\u25B8" : "\u25BE";
      }
    }
  });
}

function expandNamespace(ns: string): void {
  const children =
    document.querySelector(`[data-ns-children="${ns}"]`) ??
    document.querySelector(`[data-ns-layers="${ns}"]`);
  const caret = document.querySelector(`[data-caret="${ns}"]`);
  if (children && caret) {
    children.classList.remove("collapsed");
    caret.classList.remove("collapsed");
    caret.textContent = "\u25BE";
  }
}

/**
 * Sync namespace checkbox state based on descendant table checkboxes.
 * Bubbles up to parent namespaces for nested hierarchies.
 */
function syncNsCheckbox(nsPath: string): void {
  const directInputs = document.querySelectorAll<HTMLInputElement>(
    `input[data-ns="${nsPath}"][data-layer]`
  );
  const nestedInputs = document.querySelectorAll<HTMLInputElement>(
    `input[data-ns^="${nsPath}."][data-layer]`
  );
  const allInputs = [...directInputs, ...nestedInputs];
  const total = allInputs.length;
  const checked = allInputs.filter((i) => i.checked).length;
  const nsInput = document.querySelector<HTMLInputElement>(
    `input[data-ns-check="${nsPath}"]`
  );
  if (nsInput) {
    nsInput.checked = checked > 0;
    nsInput.indeterminate = checked > 0 && checked < total;
  }

  // Bubble up to parent namespace
  const lastDot = nsPath.lastIndexOf(".");
  if (lastDot > 0) {
    syncNsCheckbox(nsPath.slice(0, lastDot));
  }
}

// ---------------------------------------------------------------------------
// Layer count / loading / checked helpers
// ---------------------------------------------------------------------------

export function updateTreeLayerCount(
  ns: string,
  layer: string,
  count: number
): void {
  const el = document.querySelector(`[data-lcount="${ns}/${layer}"]`);
  if (el) el.textContent = count.toLocaleString();
}

export function setTreeLayerLoading(
  ns: string,
  layer: string,
  loading: boolean
): void {
  const el = document.querySelector(`[data-lcount="${ns}/${layer}"]`);
  if (el && loading) el.textContent = "\u2026";
}

export function setTreeLayerChecked(
  ns: string,
  layer: string,
  checked: boolean
): void {
  const input = document.querySelector<HTMLInputElement>(
    `input[data-ns="${ns}"][data-layer="${layer}"]`
  );
  if (input) {
    input.checked = checked;
    syncNsCheckbox(ns);
  }
}

// ---------------------------------------------------------------------------
// Status bar
// ---------------------------------------------------------------------------

export function setStatus(message: string): void {
  const el = document.getElementById("status")!;
  el.textContent = message;
}

// ---------------------------------------------------------------------------
// Debug log overlay (visible on-screen, toggled with ?debug=1)
// ---------------------------------------------------------------------------

const DEBUG_ENABLED =
  typeof window !== "undefined" &&
  new URLSearchParams(window.location.search).has("debug");

let debugPanel: HTMLElement | null = null;

function ensureDebugPanel(): HTMLElement {
  if (debugPanel) return debugPanel;
  debugPanel = document.createElement("div");
  debugPanel.id = "debug-log";
  Object.assign(debugPanel.style, {
    position: "fixed",
    bottom: "28px",
    left: "0",
    right: "0",
    maxHeight: "200px",
    overflowY: "auto",
    background: "rgba(0,0,0,0.85)",
    color: "#0f0",
    fontFamily: "monospace",
    fontSize: "11px",
    padding: "4px 8px",
    zIndex: "9999",
    pointerEvents: "auto",
  });
  document.body.appendChild(debugPanel);
  return debugPanel;
}

/** Append a debug message to the on-screen log (only when ?debug=1). */
export function debugLog(
  msg: string,
  level: "info" | "warn" | "err" = "info"
): void {
  console.log(`[webmap-debug] ${msg}`);
  if (!DEBUG_ENABLED) return;
  const panel = ensureDebugPanel();
  const line = document.createElement("div");
  line.textContent = `${new Date().toISOString().slice(11, 23)} ${msg}`;
  if (level === "err") line.style.color = "#f44";
  if (level === "warn") line.style.color = "#ff4";
  panel.appendChild(line);
  panel.scrollTop = panel.scrollHeight;
}

// ---------------------------------------------------------------------------
// Popup (single-click)
// ---------------------------------------------------------------------------

export function showPopup(
  props: Record<string, unknown>,
  x: number,
  y: number
): void {
  const popup = document.getElementById("popup")!;
  const entries = Object.entries(props)
    .map(
      ([k, v]) =>
        `<div class="popup-row"><span class="popup-key">${k}</span> ${v}</div>`
    )
    .join("");
  popup.innerHTML = entries;
  popup.style.left = `${x + 12}px`;
  popup.style.top = `${y - 12}px`;
  popup.classList.remove("hidden");
}

export function hidePopup(): void {
  document.getElementById("popup")!.classList.add("hidden");
}

// ---------------------------------------------------------------------------
// Identify Mode
// ---------------------------------------------------------------------------

export type IdentifyToggleCallback = (active: boolean) => void;

export function initIdentifyToggle(
  onToggle: IdentifyToggleCallback
): void {
  // Append to catalog toolbar if it exists, otherwise fall back to controls-inner
  const toolbar =
    document.querySelector(".catalog-toolbar") ??
    document.querySelector(".controls-inner");
  if (!toolbar) return;

  const section = document.createElement("div");
  section.className = "identify-section";
  section.innerHTML = `
    <button id="identify-btn" class="identify-btn" title="Identify features">
      <span class="identify-icon">&#9432;</span>
      <span class="identify-label">Identify</span>
    </button>
  `;
  toolbar.appendChild(section);

  document.getElementById("identify-btn")!.addEventListener("click", () => {
    const isActive = document
      .getElementById("identify-btn")!
      .classList.toggle("active");
    onToggle(isActive);
  });
}

export function showIdentifyPanel(onClear: () => void): void {
  const panel = document.getElementById("identify-panel")!;
  panel.innerHTML = `
    <div class="identify-header">
      <span class="identify-title">Identify Results</span>
      <button id="identify-clear-btn" class="identify-clear-btn">Clear</button>
    </div>
    <div id="identify-results" class="identify-results"></div>
  `;
  panel.classList.remove("hidden");
  document
    .getElementById("identify-clear-btn")!
    .addEventListener("click", onClear);
}

export function hideIdentifyPanel(): void {
  const panel = document.getElementById("identify-panel")!;
  panel.classList.add("hidden");
  panel.innerHTML = "";
}

export function addIdentifyResult(props: Record<string, unknown>): void {
  const container = document.getElementById("identify-results");
  if (!container) return;

  const card = document.createElement("div");
  card.className = "identify-card";

  const featureType = props.type ?? "Feature";
  const entries = Object.entries(props)
    .filter(([k]) => k !== "type")
    .map(
      ([k, v]) =>
        `<div class="popup-row"><span class="popup-key">${k}</span> ${v}</div>`
    )
    .join("");

  card.innerHTML = `
    <div class="identify-card-header">${featureType}</div>
    ${entries}
  `;
  container.appendChild(card);
  container.scrollTop = container.scrollHeight;
}

export function clearIdentifyResults(): void {
  const container = document.getElementById("identify-results");
  if (container) container.innerHTML = "";
}

export function deactivateIdentifyButton(): void {
  document.getElementById("identify-btn")?.classList.remove("active");
}

// ---------------------------------------------------------------------------
// Basemap Picker
// ---------------------------------------------------------------------------

export type BasemapChangeCallback = (index: number) => void;

export function initBasemapPicker(
  basemaps: Array<{ name: string; style: string }>,
  currentIndex: number,
  onChange: BasemapChangeCallback
): void {
  const select = document.getElementById("basemap-select") as HTMLSelectElement | null;
  if (!select) return;

  select.innerHTML = basemaps
    .map(
      (b, i) =>
        `<option value="${i}" ${i === currentIndex ? "selected" : ""}>${b.name}</option>`
    )
    .join("");

  select.addEventListener("change", (e) => {
    const index = parseInt((e.target as HTMLSelectElement).value, 10);
    onChange(index);
  });
}

// ---------------------------------------------------------------------------
// Layer Context Menu (right-click on table nodes)
// ---------------------------------------------------------------------------

let activeContextMenu: HTMLElement | null = null;

function dismissContextMenu(): void {
  if (activeContextMenu) {
    activeContextMenu.remove();
    activeContextMenu = null;
  }
}

function initContextMenu(
  container: HTMLElement,
  onZoom: (ns: string, layer: string) => void,
  onOpenAttributeTable?: OpenAttributeTableCallback,
  onOpenSymbology?: OpenSymbologyCallback,
): void {
  container.addEventListener("contextmenu", (e) => {
    const tableNode = (e.target as HTMLElement).closest(".cat-table");
    if (!tableNode) return;

    // Prevent default browser context menu
    e.preventDefault();
    e.stopPropagation();

    // Right-clicking a <label> causes browsers to fire a synthetic click
    // on the associated checkbox. Intercept and cancel that one-time click.
    const cb = tableNode.querySelector<HTMLInputElement>("input[type=checkbox]");
    if (cb) {
      cb.addEventListener("click", (ev) => { ev.preventDefault(); }, { once: true });
    }

    dismissContextMenu();

    const input = tableNode.querySelector<HTMLInputElement>("input[data-layer]");
    if (!input) return;

    const ns = input.dataset.ns!;
    const table = input.dataset.layer!;

    // Compute Esri layer index: position of this table among sibling .cat-table nodes
    const parent = tableNode.parentElement;
    let layerIndex = 0;
    if (parent) {
      const siblings = parent.querySelectorAll(":scope > .cat-table");
      for (let i = 0; i < siblings.length; i++) {
        if (siblings[i] === tableNode) { layerIndex = i; break; }
      }
    }

    // OGC collection name: dots → dashes
    const ogcCollection = `${ns.replace(/\./g, "-")}-${table}`;

    const menu = document.createElement("div");
    menu.className = "layer-context-menu";
    menu.style.left = `${e.clientX}px`;
    menu.style.top = `${e.clientY}px`;
    menu.innerHTML = `
      <a href="#" class="ctx-zoom">Zoom to extent</a>
      ${onOpenAttributeTable ? '<a href="#" class="ctx-attr-table">Open attribute table</a>' : ''}
      ${onOpenSymbology ? '<a href="#" class="ctx-symbology">Style layer...</a>' : ''}
      <div class="ctx-divider"></div>
      <div class="ctx-label">API Endpoints</div>
      <a href="/api/features/${ns}/${table}?limit=10" target="_blank">Feature API</a>
      <a href="/ogc/collections/${ogcCollection}/items" target="_blank">OGC API Features</a>
      <a href="/esri/rest/services/${ns}/FeatureServer/${layerIndex}" target="_blank">Esri GeoServices</a>
      <div class="ctx-divider"></div>
      <div class="ctx-label">Coordinate Reference System</div>
      <span class="ctx-info">Storage: EPSG:4326 (WGS 84)</span>
      <span class="ctx-info">Display: EPSG:3857 (Web Mercator)</span>
    `;
    menu.querySelector(".ctx-zoom")!.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      onZoom(ns, table);
    });
    menu.querySelector(".ctx-attr-table")?.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      onOpenAttributeTable!(ns, table);
    });
    menu.querySelector(".ctx-symbology")?.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      onOpenSymbology!(ns, table);
    });
    document.body.appendChild(menu);
    activeContextMenu = menu;

    // Keep menu in viewport
    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth) {
      menu.style.left = `${e.clientX - rect.width}px`;
    }
    if (rect.bottom > window.innerHeight) {
      menu.style.top = `${e.clientY - rect.height}px`;
    }
  });

  // Dismiss only on clicks outside the menu
  document.addEventListener("mousedown", (e) => {
    if (activeContextMenu && !activeContextMenu.contains(e.target as Node)) {
      dismissContextMenu();
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") dismissContextMenu();
  });
}

// ---------------------------------------------------------------------------
// Search Results Dropdown
// ---------------------------------------------------------------------------

export interface SearchResult {
  name: string;
  lat: number;
  lon: number;
  bbox?: [number, number, number, number];
}

export function showSearchResults(
  results: SearchResult[],
  onSelect: (result: SearchResult) => void
): void {
  const dropdown = document.querySelector(".search-results") as HTMLElement;
  if (!dropdown) return;

  dropdown.innerHTML = results
    .map(
      (r, i) =>
        `<div class="search-result-item" data-idx="${i}">${r.name}</div>`
    )
    .join("");
  dropdown.style.display = results.length > 0 ? "block" : "none";

  dropdown.addEventListener("click", (e) => {
    const item = (e.target as HTMLElement).closest(".search-result-item") as HTMLElement;
    if (!item) return;
    const idx = parseInt(item.dataset.idx!, 10);
    onSelect(results[idx]);
    hideSearchResults();
  });
}

export function hideSearchResults(): void {
  const dropdown = document.querySelector(".search-results") as HTMLElement;
  if (dropdown) dropdown.style.display = "none";
}

/** Deactivate measure buttons in the toolbar */
export function deactivateMeasureButtons(): void {
  document.getElementById("measure-dist-btn")?.classList.remove("active");
  document.getElementById("measure-area-btn")?.classList.remove("active");
}

// ---------------------------------------------------------------------------
// Active Layers Legend (floating cards on map)
// ---------------------------------------------------------------------------

export interface ActiveLayerInfo {
  key: string;
  name: string;
  count: number;
  color: string;
  visible: boolean;
}

export interface ActiveLayerCallbacks {
  onToggleVisibility: (key: string) => void;
  onRemove: (key: string) => void;
  onReorder: (orderedKeys: string[]) => void;
  onOpacityChange?: (key: string, opacity: number) => void;
  onZoom?: (ns: string, layer: string) => void;
  onOpenAttributeTable?: (ns: string, layer: string) => void;
  onOpenSymbology?: (ns: string, layer: string) => void;
  onDeleteScratch?: (key: string) => void;
  onSaveScratch?: (key: string) => void;
}

let activeLayerCallbacks: ActiveLayerCallbacks | null = null;
let legendEventsWired = false;
let legendDraggedKey: string | null = null;

export function initActiveLayers(callbacks: ActiveLayerCallbacks): void {
  activeLayerCallbacks = callbacks;
}

/**
 * Wire event listeners on the legend container ONCE.  Called on first render
 * only; all handlers use event delegation so they work with any card HTML
 * that gets swapped in later by renderActiveLayers().
 */
function wireLegendEvents(container: HTMLElement): void {
  if (legendEventsWired) return;
  legendEventsWired = true;

  // Click actions (toggle visibility, remove)
  container.addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest("[data-action]") as HTMLElement;
    if (!btn) return;
    const card = btn.closest(".active-layer-card") as HTMLElement;
    if (!card) return;
    const key = card.dataset.layerKey!;
    if (btn.dataset.action === "toggle") {
      activeLayerCallbacks?.onToggleVisibility(key);
    } else if (btn.dataset.action === "remove") {
      activeLayerCallbacks?.onRemove(key);
    }
  });

  // Opacity slider
  container.addEventListener("input", (e) => {
    const target = e.target as HTMLInputElement;
    if (!target.classList.contains("sidebar-layer-opacity")) return;
    const key = target.dataset.opacityKey;
    if (key && activeLayerCallbacks?.onOpacityChange) {
      activeLayerCallbacks.onOpacityChange(key, parseInt(target.value, 10) / 100);
    }
  });

  // Drag-and-drop reorder
  // Prevent child interactive elements from hijacking drag
  container.addEventListener("mousedown", (e) => {
    const target = e.target as HTMLElement;
    if (target.tagName === "INPUT" || target.tagName === "BUTTON" || target.closest("button")) return;
    const card = target.closest(".active-layer-card") as HTMLElement;
    if (card) card.setAttribute("draggable", "true");
  });

  container.addEventListener("dragstart", (e) => {
    const card = (e.target as HTMLElement).closest(".active-layer-card") as HTMLElement;
    if (!card) return;
    legendDraggedKey = card.dataset.layerKey!;
    card.classList.add("dragging");
    const dt = (e as DragEvent).dataTransfer!;
    dt.effectAllowed = "move";
    dt.setData("text/plain", legendDraggedKey);
  });

  container.addEventListener("dragend", (e) => {
    const card = (e.target as HTMLElement).closest(".active-layer-card") as HTMLElement;
    if (card) card.classList.remove("dragging");
    container.querySelectorAll(".drag-over").forEach((el) => el.classList.remove("drag-over"));
    legendDraggedKey = null;
  });

  container.addEventListener("dragover", (e) => {
    e.preventDefault();
    (e as DragEvent).dataTransfer!.dropEffect = "move";
    const card = (e.target as HTMLElement).closest(".active-layer-card") as HTMLElement;
    if (card && card.dataset.layerKey !== legendDraggedKey) {
      container.querySelectorAll(".drag-over").forEach((el) => el.classList.remove("drag-over"));
      card.classList.add("drag-over");
    }
  });

  container.addEventListener("drop", (e) => {
    e.preventDefault();
    container.querySelectorAll(".drag-over").forEach((el) => el.classList.remove("drag-over"));
    const targetCard = (e.target as HTMLElement).closest(".active-layer-card") as HTMLElement;
    if (!targetCard || !legendDraggedKey) return;
    const targetKey = targetCard.dataset.layerKey!;
    if (targetKey === legendDraggedKey) return;

    const cards = [...container.querySelectorAll(".active-layer-card")] as HTMLElement[];
    const keys = cards.map((c) => c.dataset.layerKey!);
    const fromIdx = keys.indexOf(legendDraggedKey);
    const toIdx = keys.indexOf(targetKey);
    keys.splice(fromIdx, 1);
    keys.splice(toIdx, 0, legendDraggedKey);
    activeLayerCallbacks?.onReorder(keys);
  });

  // Right-click context menu
  container.addEventListener("contextmenu", (e) => {
    const card = (e.target as HTMLElement).closest(".active-layer-card") as HTMLElement;
    if (!card) return;
    e.preventDefault();
    e.stopPropagation();
    dismissContextMenu();

    const key = card.dataset.layerKey!;
    const slashIdx = key.indexOf("/");
    const ns = key.slice(0, slashIdx);
    const table = key.slice(slashIdx + 1);
    const ogcCollection = `${ns.replace(/\./g, "-")}-${table}`;
    const isScratch = ns.startsWith("_scratch_");

    const menu = document.createElement("div");
    menu.className = "layer-context-menu";
    menu.style.left = `${(e as MouseEvent).clientX}px`;
    menu.style.top = `${(e as MouseEvent).clientY}px`;
    menu.innerHTML = `
      <a href="#" class="ctx-zoom">Zoom to extent</a>
      <a href="#" class="ctx-attr-table">Open attribute table</a>
      <a href="#" class="ctx-symbology">Style layer\u2026</a>
      ${isScratch ? `
        <div class="ctx-divider"></div>
        <div class="ctx-label">Scratch Layer</div>
        <a href="#" class="ctx-save-scratch">Save to permanent layer\u2026</a>
        <a href="#" class="ctx-delete-scratch">Delete from lakehouse</a>
      ` : ''}
      <div class="ctx-divider"></div>
      <div class="ctx-label">API Endpoints</div>
      <a href="/api/features/${ns}/${table}?limit=10" target="_blank">Feature API</a>
      <a href="/ogc/collections/${ogcCollection}/items" target="_blank">OGC API Features</a>
      <a href="/esri/rest/services/${ns}/FeatureServer" target="_blank">Esri GeoServices</a>
      <div class="ctx-divider"></div>
      <div class="ctx-label">Coordinate Reference System</div>
      <span class="ctx-info">Storage: EPSG:4326 (WGS 84)</span>
      <span class="ctx-info">Display: EPSG:3857 (Web Mercator)</span>
    `;
    menu.querySelector(".ctx-zoom")!.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      activeLayerCallbacks?.onZoom?.(ns, table);
    });
    menu.querySelector(".ctx-attr-table")!.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      activeLayerCallbacks?.onOpenAttributeTable?.(ns, table);
    });
    menu.querySelector(".ctx-symbology")!.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      activeLayerCallbacks?.onOpenSymbology?.(ns, table);
    });
    menu.querySelector(".ctx-save-scratch")?.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      activeLayerCallbacks?.onSaveScratch?.(key);
    });
    menu.querySelector(".ctx-delete-scratch")?.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      activeLayerCallbacks?.onDeleteScratch?.(key);
    });
    document.body.appendChild(menu);
    activeContextMenu = menu;

    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth) {
      menu.style.left = `${(e as MouseEvent).clientX - rect.width}px`;
    }
    if (rect.bottom > window.innerHeight) {
      menu.style.top = `${(e as MouseEvent).clientY - rect.height}px`;
    }
  });

  // Global dismiss: click outside or Escape key
  document.addEventListener("mousedown", (e) => {
    if (activeContextMenu && !activeContextMenu.contains(e.target as Node)) {
      dismissContextMenu();
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") dismissContextMenu();
  });
}

export function renderActiveLayers(layers: ActiveLayerInfo[]): void {
  const container = document.getElementById("sidebar-layers") ?? document.getElementById("active-layers");
  if (!container) return;

  // Wire delegated events once on first render
  wireLegendEvents(container);

  if (layers.length === 0) {
    container.innerHTML = '<div class="sidebar-no-layers">No layers loaded</div>';
    return;
  }

  container.innerHTML = layers
    .map(
      (l) => `
      <div class="active-layer-card" draggable="true" data-layer-key="${l.key}">
        <span class="active-layer-handle">\u2630</span>
        <span class="active-layer-swatch" style="background:${l.color}; opacity:${l.visible ? 1 : 0.3}"></span>
        <span class="active-layer-name" title="${l.key}">${l.name}</span>
        <span class="active-layer-count">${l.count.toLocaleString()}</span>
        <input type="range" class="sidebar-layer-opacity" data-opacity-key="${l.key}" min="0" max="100" value="100" title="Opacity" draggable="false" />
        <button class="active-layer-vis" data-action="toggle" title="${l.visible ? 'Hide' : 'Show'}" draggable="false">${l.visible ? '\u{1F441}' : '\u25CB'}</button>
        <button class="active-layer-remove" data-action="remove" title="Remove" draggable="false">\u2715</button>
      </div>`
    )
    .join("");
}

// ---------------------------------------------------------------------------
// Catalog Modal
// ---------------------------------------------------------------------------

export function openCatalogModal(): void {
  const modal = document.getElementById("catalog-modal");
  if (modal) modal.classList.remove("hidden");
}

export function closeCatalogModal(): void {
  const modal = document.getElementById("catalog-modal");
  if (modal) modal.classList.add("hidden");
}

export function initCatalogModal(): void {
  const modal = document.getElementById("catalog-modal");
  if (!modal) return;

  modal.querySelector(".catalog-modal-backdrop")?.addEventListener("click", closeCatalogModal);
  modal.querySelector(".catalog-modal-close")?.addEventListener("click", closeCatalogModal);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && !modal.classList.contains("hidden")) {
      closeCatalogModal();
    }
  });
}
