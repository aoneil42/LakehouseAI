# Bundled Basemap Tiles

For disconnected/air-gapped deployments, place `.mbtiles` vector tile files in this directory.
The `tileserver-gl-light` container auto-discovers and serves them.

## Downloading Tiles

**OpenMapTiles (recommended):**
Download pre-built vector tiles from https://data.maptiler.com/downloads/planet/

- Global (z0-14): ~80 GB
- Regional extracts available (e.g., North America, Europe)
- Format: OpenMapTiles schema (compatible with `terminus-dark.json` style)

**Natural Earth (lightweight, ~100 MB):**
Good for low-zoom global coverage. Download from:
https://klokantech.github.io/naturalearthtiles/

## Quick Start

```bash
# Example: download a small regional extract
# (replace with your region of interest)
wget -O tiles/planet.mbtiles "https://data.maptiler.com/download/..."

# Start tileserver
docker compose --profile disconnected up -d

# Verify
curl http://localhost:8070/styles/terminus-dark/style.json
```

## Custom Tiles from Esri

Export vector tile packages (.vtpk) from ArcGIS Pro, convert to .mbtiles using
`ogr2ogr` or the `vtpk2mbtiles` tool, and place in this directory.

## Configuration

- `config.json` — tileserver-gl configuration (maps mbtiles to sources)
- `terminus-dark.json` — Terminus-branded dark MapLibre style

The webmap uses `basemaps-disconnected.json` to point at the tileserver.
Set `BASEMAP_CONFIG=basemaps-disconnected.json` when building the webmap container.
