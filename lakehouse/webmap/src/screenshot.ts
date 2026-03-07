/**
 * Map screenshot — composites MapLibre + deck.gl canvases into a single PNG.
 * Requires preserveDrawingBuffer: true on the MapLibre map.
 */

export function captureMap(mapCanvas: HTMLCanvasElement): void {
  const deckCanvas = document.querySelector(
    ".deck-widget-container canvas"
  ) as HTMLCanvasElement | null;

  const dpr = window.devicePixelRatio || 1;
  const composite = document.createElement("canvas");
  composite.width = mapCanvas.width;
  composite.height = mapCanvas.height;
  const ctx = composite.getContext("2d")!;

  // Draw basemap
  ctx.drawImage(mapCanvas, 0, 0);

  // Draw deck.gl overlay
  if (deckCanvas) {
    ctx.drawImage(deckCanvas, 0, 0, composite.width, composite.height);
  }

  // Add timestamp watermark
  ctx.font = `${12 * dpr}px sans-serif`;
  ctx.fillStyle = "rgba(255,255,255,0.6)";
  ctx.textAlign = "right";
  ctx.fillText(
    new Date().toISOString().slice(0, 19),
    composite.width - 10 * dpr,
    composite.height - 10 * dpr
  );

  // Trigger download
  composite.toBlob((blob) => {
    if (!blob) return;
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `map-${Date.now()}.png`;
    a.click();
    URL.revokeObjectURL(url);
  }, "image/png");
}
