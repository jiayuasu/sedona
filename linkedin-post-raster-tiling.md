# LinkedIn post — Sedona 1.9.0 raster tiling data source

Status: final draft, ready to copy. The post body is everything between the two `---` rules.

---

The hardest part of working with big rasters often isn't the analysis. It's just getting the file open.

Load one massive GeoTIFF and the job dies — not because your cluster is too small, but because the engine underneath won't materialize a single record larger than 2 GB. And Apache Sedona didn't save you: Sedona runs on Spark, so it inherited that ceiling. Its raster loader read each file as one record. Take a real one: the annual VIIRS nighttime-lights composite — Earth at night, one global GeoTIFF, 86,400 × 33,600 pixels, ~11.6 GB decompressed. That entire map of human civilization became a single row that toppled over before any spatial work began.

You were left with ugly workarounds: hand-tile the file first, convert formats offline, or throw memory at it and pray.

Apache Sedona 1.9.0 removes the wall. A new raster data source tiles a GeoTIFF as it reads — one row per tile, streamed straight into a DataFrame:

df = sedona.read.format("raster").load("/data/huge.tif")

That's the whole thing. No giant record, no OOM. Each tile keeps its (x, y) position, and Sedona spreads them across the cluster automatically so one executor doesn't carry the whole file alone.

A few things worth knowing:
• COGs shine here — their square internal tiles map cleanly onto the reader.
• You stay in control: option("retile", "false") for whole rasters, or set tileWidth / tileHeight yourself.
• It globs and walks directories too (recursiveFileLookup, pathGlobFilter) — a folder of thousands of scenes is one line.

Whether you work in Earth observation, climate, agriculture, or anything that ships pixels at scale, this quietly deletes a problem teams have been engineering around for years.

Release notes 👉 https://sedona.apache.org/latest/setup/release-notes/
If you've hit the big-raster wall, try it — I want to know how big you can go.

#ApacheSedona #Geospatial #RasterData #EarthObservation #RemoteSensing #OpenSource

---

## Fact-check notes (strip before posting)

- **Feature**: [GH-2672] "Add a new raster data source reader that automatically tiles GeoTiffs to bypass Spark's 2GB record size limit and avoid OOM issues when loading single large rasters", merged in apache/sedona#2673, listed in the 1.9.0 release-notes highlights.
- **Reader options** (verified against the merged docs): `retile` (default `true`), `tileWidth`, `tileHeight` (defaults to the file's internal tile size when unset), `padWithNoData` (default `false`). Standard Spark file-source options (`recursiveFileLookup`, `pathGlobFilter`) apply as with any file data source.
- **Output schema**: `rast` (Raster), `x` / `y` (0-based tile position, present when retiling), `name` (file name) — so "each tile keeps its (x, y) position" is accurate.
- **Named scene**: Earth Observation Group (Colorado School of Mines) annual VNL V2 composites are distributed as single global (non-tiled) gzipped GeoTIFFs, 15 arc-second grid, 180W–180E / 75N–65S → 86,400 × 33,600 pixels; float32 ≈ 11.6 GB decompressed (matches the 11.61 GB figure reported by the GEE community catalog). Source: https://eogdata.mines.edu/products/vnl/
- **COG recommendation**: the Sedona docs themselves recommend COG for this reader because pixel data is organized as square internal tiles.
