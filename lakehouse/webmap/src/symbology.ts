/**
 * Per-layer symbology panel — color, opacity, radius, stroke controls.
 */

import type { Color } from "@deck.gl/core";
import type { GeomType } from "./layers";

export interface LayerStyle {
  fillColor: Color;
  strokeColor: Color;
  strokeWidth: number;
  radius: number;
  opacity: number;
}

export function getDefaultStyle(geomType: GeomType): LayerStyle {
  switch (geomType) {
    case "point":
      return {
        opacity: 1,
        fillColor: [100, 100, 100, 255],
        strokeColor: [255, 255, 255, 200],
        strokeWidth: 1,
        radius: 300,
      };
    case "line":
      return {
        opacity: 1,
        fillColor: [80, 80, 80, 200],
        strokeColor: [80, 80, 80, 200],
        strokeWidth: 2,
        radius: 0,
      };
    case "polygon":
      return {
        opacity: 0.63,
        fillColor: [30, 144, 255, 160],
        strokeColor: [0, 0, 0, 255],
        strokeWidth: 1,
        radius: 0,
      };
    default:
      return {
        opacity: 1,
        fillColor: [100, 100, 100, 255],
        strokeColor: [100, 100, 100, 255],
        strokeWidth: 1,
        radius: 0,
      };
  }
}

function rgbToHex(rgba: Color): string {
  return (
    "#" +
    [rgba[0], rgba[1], rgba[2]]
      .map((c) => c.toString(16).padStart(2, "0"))
      .join("")
  );
}

function hexToRgba(
  hex: string,
  alpha: number
): [number, number, number, number] {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return [r, g, b, alpha];
}

export class SymbologyPanel {
  private container: HTMLDivElement;

  constructor(
    private layerKey: string,
    private geomType: GeomType,
    private style: LayerStyle,
    private onChange: (style: LayerStyle) => void,
    private onCloseCallback: () => void
  ) {
    this.container = document.createElement("div");
    this.container.className = "symbology-panel";
    this.buildUI();
  }

  mount(parent: HTMLElement): void {
    parent.appendChild(this.container);
  }

  destroy(): void {
    this.container.remove();
  }

  private buildUI(): void {
    const s = this.style;
    let controlsHtml = `
      <div class="sym-row">
        <label>Fill Color</label>
        <input type="color" class="sym-fill-color" value="${rgbToHex(s.fillColor)}" />
      </div>
      <div class="sym-row">
        <label>Opacity</label>
        <input type="range" class="sym-opacity" min="0" max="100" value="${Math.round(s.opacity * 100)}" />
        <span class="sym-opacity-val">${Math.round(s.opacity * 100)}%</span>
      </div>
    `;

    if (this.geomType === "point") {
      controlsHtml += `
        <div class="sym-row">
          <label>Radius</label>
          <input type="range" class="sym-radius" min="50" max="5000" value="${s.radius}" />
        </div>
      `;
    }

    if (this.geomType === "line") {
      controlsHtml += `
        <div class="sym-row">
          <label>Width</label>
          <input type="range" class="sym-width" min="1" max="10" value="${s.strokeWidth}" />
        </div>
      `;
    }

    this.container.innerHTML = `
      <div class="symbology-header">
        <span>Style: ${this.layerKey}</span>
        <button class="symbology-close">&#x2715;</button>
      </div>
      <div class="symbology-body">${controlsHtml}</div>
    `;

    // Wire events
    this.container
      .querySelector(".symbology-close")!
      .addEventListener("click", () => {
        this.destroy();
        this.onCloseCallback();
      });

    this.container
      .querySelector(".sym-fill-color")
      ?.addEventListener("input", (e) => {
        const hex = (e.target as HTMLInputElement).value;
        this.style.fillColor = hexToRgba(hex, this.style.fillColor[3] ?? 255);
        this.onChange(this.style);
      });

    this.container
      .querySelector(".sym-opacity")
      ?.addEventListener("input", (e) => {
        const val = parseInt((e.target as HTMLInputElement).value);
        this.style.opacity = val / 100;
        this.container.querySelector(".sym-opacity-val")!.textContent = `${val}%`;
        this.onChange(this.style);
      });

    this.container
      .querySelector(".sym-radius")
      ?.addEventListener("input", (e) => {
        this.style.radius = parseInt((e.target as HTMLInputElement).value);
        this.onChange(this.style);
      });

    this.container
      .querySelector(".sym-width")
      ?.addEventListener("input", (e) => {
        this.style.strokeWidth = parseInt(
          (e.target as HTMLInputElement).value
        );
        this.onChange(this.style);
      });
  }
}
