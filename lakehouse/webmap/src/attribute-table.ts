/**
 * Virtual-scroll attribute table panel for browsing Arrow table data.
 * Mounts as a slide-up panel at the bottom of the map container.
 */

import type { Table } from "apache-arrow";
import { DataType } from "apache-arrow";

const VISIBLE_ROWS = 50;
const ROW_HEIGHT = 28;
const DEFAULT_COL_WIDTH = 150;
const MIN_COL_WIDTH = 50;

// Timestamp/date column format hints derived from Arrow schema
type ColFormat =
  | "ts-s"    // Timestamp(SECOND)
  | "ts-ms"   // Timestamp(MILLISECOND) or heuristic bigint epoch-ms
  | "ts-us"   // Timestamp(MICROSECOND)
  | "ts-ns"   // Timestamp(NANOSECOND)
  | "date-d"  // Date(DAY)
  | "date-ms" // Date(MILLISECOND)
  | null;

/** Epoch-ms range check for heuristic timestamp detection (years ~1990–2100). */
const EPOCH_MS_MIN = 631_152_000_000n;   // 1990-01-01
const EPOCH_MS_MAX = 4_102_444_800_000n;  // 2100-01-01

/** Column name patterns that hint at epoch-ms bigint timestamps. */
const TS_NAME_RE = /timestamp|created|updated|modified|_at$|_time$|_date$|datetime|epoch/i;

export interface AttributeTableOptions {
  onClose: () => void;
  onRowHover?: (index: number | null) => void;
  onRowClick?: (index: number) => void;
}

export class AttributeTable {
  private container: HTMLDivElement;
  private table: Table;
  private columns: string[];
  private colWidths: number[];
  private colFormats: Map<string, ColFormat>;
  private sortColumn: string | null = null;
  private sortAsc = true;
  private sortedIndices: number[];
  private filterText = "";
  private layerKey: string;
  private tooltip: HTMLDivElement;
  private tooltipTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    arrowTable: Table,
    layerKey: string,
    private options: AttributeTableOptions
  ) {
    this.table = arrowTable;
    this.layerKey = layerKey;
    this.columns = arrowTable.schema.fields
      .map((f) => f.name)
      .filter((name) => name !== "geometry");

    this.colWidths = this.columns.map(() => DEFAULT_COL_WIDTH);

    // Detect timestamp/date columns from Arrow schema
    this.colFormats = new Map();
    const tsUnits: ColFormat[] = ["ts-s", "ts-ms", "ts-us", "ts-ns"];
    for (const field of arrowTable.schema.fields) {
      if (field.name === "geometry") continue;
      const t = field.type;
      if (DataType.isTimestamp(t)) {
        this.colFormats.set(field.name, tsUnits[(t as any).unit] ?? "ts-ms");
      } else if (DataType.isDate(t)) {
        // DateMillisecond vs DateDay
        this.colFormats.set(field.name, t.typeId === -14 ? "date-ms" : "date-d");
      } else if (DataType.isInt(t) && (t as any).bitWidth === 64) {
        // Heuristic: BIGINT columns with timestamp-like names
        if (TS_NAME_RE.test(field.name)) {
          // Verify first non-null value looks like epoch-ms
          const col = arrowTable.getChild(field.name);
          if (col) {
            for (let i = 0; i < Math.min(arrowTable.numRows, 10); i++) {
              const v = col.get(i);
              if (v != null) {
                const bv = typeof v === "bigint" ? v : BigInt(v);
                if (bv >= EPOCH_MS_MIN && bv <= EPOCH_MS_MAX) {
                  this.colFormats.set(field.name, "ts-ms");
                }
                break;
              }
            }
          }
        }
      }
    }

    this.sortedIndices = Array.from(
      { length: arrowTable.numRows },
      (_, i) => i
    );

    this.container = document.createElement("div");
    this.container.className = "attr-table-panel";

    const colgroup = this.columns
      .map((_, i) => `<col style="width:${this.colWidths[i]}px" />`)
      .join("");

    this.container.innerHTML = `
      <div class="attr-table-header">
        <div class="attr-table-title">
          <span class="attr-table-layer">${layerKey}</span>
          <span class="attr-table-count">${arrowTable.numRows.toLocaleString()} features</span>
        </div>
        <div class="attr-table-actions">
          <input type="text" class="attr-table-filter" placeholder="Filter..." />
          <button class="attr-table-export-btn" title="Export to CSV">CSV</button>
          <button class="attr-table-close-btn" title="Close">&#x2715;</button>
        </div>
      </div>
      <div class="attr-table-scroll">
        <table class="attr-table">
          <colgroup>${colgroup}</colgroup>
          <thead>
            <tr>${this.columns
              .map(
                (c) =>
                  `<th data-col="${c}" class="attr-th">${c} <span class="sort-arrow"></span><div class="col-resize-handle"></div></th>`
              )
              .join("")}</tr>
          </thead>
          <tbody class="attr-tbody"></tbody>
        </table>
      </div>
    `;

    // Create tooltip element
    this.tooltip = document.createElement("div");
    this.tooltip.className = "attr-cell-tooltip";
    this.container.appendChild(this.tooltip);
  }

  mount(parent: HTMLElement): void {
    parent.appendChild(this.container);
    this.wireEvents();
    this.wireResizeHandles();
    this.wireTooltip();
    this.renderRows();
  }

  destroy(): void {
    this.container.remove();
  }

  private wireEvents(): void {
    // Close button
    this.container
      .querySelector(".attr-table-close-btn")!
      .addEventListener("click", () => {
        this.destroy();
        this.options.onClose();
      });

    // Column sort (ignore clicks on resize handle)
    this.container.querySelectorAll(".attr-th").forEach((th) => {
      th.addEventListener("click", (e) => {
        if ((e.target as HTMLElement).classList.contains("col-resize-handle")) return;
        const col = (th as HTMLElement).dataset.col!;
        if (this.sortColumn === col) {
          this.sortAsc = !this.sortAsc;
        } else {
          this.sortColumn = col;
          this.sortAsc = true;
        }
        this.updateSortIndicators();
        this.applySort();
        this.renderRows();
      });
    });

    // Virtual scroll
    const scrollDiv = this.container.querySelector(".attr-table-scroll")!;
    scrollDiv.addEventListener("scroll", () => {
      this.renderRows();
    });

    // Filter
    const filterInput = this.container.querySelector(
      ".attr-table-filter"
    ) as HTMLInputElement;
    let filterTimer: ReturnType<typeof setTimeout>;
    filterInput.addEventListener("input", () => {
      clearTimeout(filterTimer);
      filterTimer = setTimeout(() => {
        this.filterText = filterInput.value.toLowerCase();
        this.applySort();
        this.renderRows();
      }, 200);
    });

    // CSV export
    this.container
      .querySelector(".attr-table-export-btn")!
      .addEventListener("click", () => this.exportCSV());

    // Row hover
    if (this.options.onRowHover) {
      const tbody = this.container.querySelector(".attr-tbody")!;
      tbody.addEventListener("mouseover", (e) => {
        const row = (e.target as HTMLElement).closest("tr");
        if (row && row.dataset.idx) {
          this.options.onRowHover!(parseInt(row.dataset.idx, 10));
        }
      });
      tbody.addEventListener("mouseleave", () => {
        this.options.onRowHover!(null);
      });
    }
  }

  private wireResizeHandles(): void {
    const handles = this.container.querySelectorAll(".col-resize-handle");
    const cols = this.container.querySelectorAll<HTMLElement>("colgroup col");

    handles.forEach((handle, i) => {
      handle.addEventListener("mousedown", (e) => {
        e.preventDefault();
        e.stopPropagation();
        const startX = (e as MouseEvent).clientX;
        const startWidth = this.colWidths[i];
        handle.classList.add("resizing");

        const onMove = (ev: MouseEvent) => {
          const delta = ev.clientX - startX;
          const newWidth = Math.max(MIN_COL_WIDTH, startWidth + delta);
          this.colWidths[i] = newWidth;
          if (cols[i]) cols[i].style.width = `${newWidth}px`;
        };

        const onUp = () => {
          handle.classList.remove("resizing");
          document.removeEventListener("mousemove", onMove);
          document.removeEventListener("mouseup", onUp);
        };

        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup", onUp);
      });
    });
  }

  private wireTooltip(): void {
    const tbody = this.container.querySelector(".attr-tbody")!;

    tbody.addEventListener("mouseover", (e) => {
      const td = (e.target as HTMLElement).closest("td") as HTMLElement;
      if (!td) return;

      const text = td.textContent ?? "";
      if (!text || td.scrollWidth <= td.clientWidth) {
        // Content fits — no tooltip needed
        return;
      }

      if (this.tooltipTimer) clearTimeout(this.tooltipTimer);
      this.tooltipTimer = setTimeout(() => {
        const rect = td.getBoundingClientRect();
        this.tooltip.textContent = text;
        this.tooltip.style.display = "block";
        this.tooltip.style.left = `${rect.left}px`;
        this.tooltip.style.top = `${rect.top - this.tooltip.offsetHeight - 4}px`;
        // Keep tooltip in viewport
        const tipRect = this.tooltip.getBoundingClientRect();
        if (tipRect.top < 0) {
          this.tooltip.style.top = `${rect.bottom + 4}px`;
        }
        if (tipRect.right > window.innerWidth) {
          this.tooltip.style.left = `${window.innerWidth - tipRect.width - 8}px`;
        }
      }, 100);
    });

    tbody.addEventListener("mouseout", (e) => {
      const td = (e.target as HTMLElement).closest("td");
      if (!td) return;
      if (this.tooltipTimer) {
        clearTimeout(this.tooltipTimer);
        this.tooltipTimer = null;
      }
      this.tooltip.style.display = "none";
    });
  }

  /** Format a cell value for display, handling timestamps/dates/numbers. */
  private formatValue(col: string, val: unknown): string {
    if (val == null) return "";

    const fmt = this.colFormats.get(col);
    if (fmt) {
      return this.formatTemporal(val, fmt);
    }

    if (typeof val === "bigint") return val.toString();
    if (typeof val === "number") {
      return Number.isInteger(val) ? val.toString() : val.toFixed(4);
    }
    // Date objects returned directly by Arrow for some Date types
    if (val instanceof Date) {
      return isNaN(val.getTime()) ? String(val) : val.toISOString().replace("T", " ").slice(0, 19);
    }
    return String(val);
  }

  /** Convert a raw temporal value to a human-readable string. */
  private formatTemporal(val: unknown, fmt: ColFormat): string {
    let ms: number;
    // Arrow may return number, bigint, or Date depending on type
    if (val instanceof Date) {
      ms = val.getTime();
    } else {
      const n = typeof val === "bigint" ? Number(val) : Number(val);
      if (isNaN(n)) return String(val);
      switch (fmt) {
        case "ts-s":    ms = n * 1000; break;
        case "ts-ms":   ms = n; break;
        case "ts-us":   ms = n / 1000; break;
        case "ts-ns":   ms = n / 1_000_000; break;
        case "date-d":  ms = n * 86_400_000; break;
        case "date-ms": ms = n; break;
        default:        return String(val);
      }
    }
    const d = new Date(ms);
    if (isNaN(d.getTime())) return String(val);
    if (fmt === "date-d" || fmt === "date-ms") {
      return d.toISOString().slice(0, 10); // YYYY-MM-DD
    }
    return d.toISOString().replace("T", " ").slice(0, 19); // YYYY-MM-DD HH:MM:SS
  }

  private updateSortIndicators(): void {
    this.container.querySelectorAll(".attr-th").forEach((th) => {
      const arrow = th.querySelector(".sort-arrow")!;
      const col = (th as HTMLElement).dataset.col;
      if (col === this.sortColumn) {
        arrow.textContent = this.sortAsc ? " \u25B2" : " \u25BC";
      } else {
        arrow.textContent = "";
      }
    });
  }

  private applySort(): void {
    const numRows = this.table.numRows;
    let indices = Array.from({ length: numRows }, (_, i) => i);

    // Filter
    if (this.filterText) {
      indices = indices.filter((i) => {
        for (const col of this.columns) {
          const val = this.table.getChild(col)?.get(i);
          if (
            val != null &&
            String(val).toLowerCase().includes(this.filterText)
          ) {
            return true;
          }
        }
        return false;
      });
    }

    // Sort
    if (this.sortColumn) {
      const col = this.table.getChild(this.sortColumn);
      if (col) {
        const asc = this.sortAsc ? 1 : -1;
        indices.sort((a, b) => {
          const va = col.get(a);
          const vb = col.get(b);
          if (va == null && vb == null) return 0;
          if (va == null) return 1;
          if (vb == null) return -1;
          if (va < vb) return -1 * asc;
          if (va > vb) return 1 * asc;
          return 0;
        });
      }
    }

    this.sortedIndices = indices;
  }

  private renderRows(): void {
    const scrollDiv = this.container.querySelector(".attr-table-scroll")!;
    const scrollTop = scrollDiv.scrollTop;
    const tbody = this.container.querySelector(".attr-tbody")!;
    const startIdx = Math.floor(scrollTop / ROW_HEIGHT);
    const endIdx = Math.min(
      startIdx + VISIBLE_ROWS,
      this.sortedIndices.length
    );

    const totalHeight = this.sortedIndices.length * ROW_HEIGHT;
    const topPad = startIdx * ROW_HEIGHT;

    let html = `<tr style="height:${topPad}px"><td colspan="${this.columns.length}"></td></tr>`;

    for (let vi = startIdx; vi < endIdx; vi++) {
      const rowIdx = this.sortedIndices[vi];
      html += `<tr data-idx="${rowIdx}">`;
      for (const col of this.columns) {
        const val = this.table.getChild(col)?.get(rowIdx);
        const display = this.formatValue(col, val);
        html += `<td>${display}</td>`;
      }
      html += "</tr>";
    }

    const bottomPad =
      totalHeight - topPad - (endIdx - startIdx) * ROW_HEIGHT;
    html += `<tr style="height:${Math.max(0, bottomPad)}px"><td colspan="${this.columns.length}"></td></tr>`;

    tbody.innerHTML = html;
  }

  private exportCSV(): void {
    const header = this.columns.join(",");
    const rows = this.sortedIndices.map((i) => {
      return this.columns
        .map((col) => {
          const val = this.table.getChild(col)?.get(i);
          if (val == null) return "";
          const str = this.formatValue(col, val);
          if (str.includes(",") || str.includes("\n") || str.includes('"')) {
            return `"${str.replace(/"/g, '""')}"`;
          }
          return str;
        })
        .join(",");
    });

    const csv = [header, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${this.layerKey.replace("/", "_")}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }
}
