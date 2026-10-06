#!/usr/bin/env python3
"""
Calcola la via di ogni fermata del feed GTFS da un estratto OpenStreetMap e la
scrive in src/main/resources/geocode/stop-addresses.csv, che il backend carica
all'avvio (StopAddressService).

Le fermate non si spostano: chiedere la loro via a un geocoder pubblico a ogni
apertura e' il modo piu' rapido per farsi limitare (e' successo con Nominatim).
Calcolata una volta qui, la via arriva subito e non dipende da nessuno.

Uso:
  ./scripts/build-stop-addresses.py --pbf ../planet_roma.osm.pbf --gtfs ../rome_static_gtfs.zip

Opzioni:
  --pbf        Estratto OSM dell'area (lo stesso usato per il grafo di OTP)
  --gtfs       Zip del feed statico, oppure direttamente stops.txt
  --out        CSV di uscita (default: src/main/resources/geocode/stop-addresses.csv)
  --boundary   Confine di Roma in GeoJSON (default: scripts/roma-boundary.geojson)

Serve `ogr2ogr` (GDAL), che legge il PBF; il resto e' libreria standard.

Va rilanciato quando il feed aggiunge o sposta fermate: una fermata che qui
manca non e' un errore, il backend per quella chiede ancora al geocoder.

Dati (c) OpenStreetMap contributors, ODbL 1.0.
"""

import argparse
import csv
import io
import json
import math
import re
import subprocess
import sys
import tempfile
import unicodedata
import zipfile
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "src/main/resources/geocode/stop-addresses.csv"
DEFAULT_BOUNDARY = ROOT / "scripts/roma-boundary.geojson"

# Strade su cui una fermata puo' stare. Le altre vie con nome (pedonali minori,
# scale, ciclabili) valgono solo se intorno non c'e' una strada vera.
ROADS = {
    "motorway", "trunk", "primary", "secondary", "tertiary", "unclassified",
    "residential", "living_street", "pedestrian", "service", "road", "busway",
    "motorway_link", "trunk_link", "primary_link", "secondary_link", "tertiary_link",
}
MINOR_WAYS = {"footway", "path", "cycleway", "steps", "track"}

# Oltre questa distanza la via piu' vicina non e' piu' "la via della fermata".
MAX_METERS = 60.0
# Il nome della fermata dice quasi sempre la via ("TAGLIAMENTO/CLITUNNO" sta su
# via Tagliamento, all'altezza di via Clitunno): a un incrocio vince quella,
# anche se la traversa e' qualche metro piu' vicina. Entro questo raggio.
NAME_MATCH_METERS = 40.0

# Metri per grado alla latitudine di Roma: su 50 km l'errore e' di centimetri.
LAT0 = 41.9
M_PER_DEG_LAT = 110_574.0
M_PER_DEG_LON = 111_320.0 * math.cos(math.radians(LAT0))
CELL_METERS = 100.0


def to_xy(lon, lat):
    return lon * M_PER_DEG_LON, lat * M_PER_DEG_LAT


def tokens(text):
    """Parole di un nome, senza accenti ne' punteggiatura: "D' OVIDIO" -> {"ovidio"}."""
    plain = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    return {t for t in re.split(r"[^a-z0-9]+", plain) if len(t) >= 3}


def export_layer(pbf, layer, where, fields, target):
    subprocess.run(
        ["ogr2ogr", "-f", "GeoJSONSeq", str(target), str(pbf), layer, "-where", where, "-select", fields],
        check=True, stdout=subprocess.DEVNULL,
    )
    with open(target, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip().lstrip("\x1e")
            if line:
                yield json.loads(line)


class StreetIndex:
    """Segmenti delle vie con nome, in una griglia da 100 m."""

    def __init__(self):
        self.cells = defaultdict(list)
        self.count = 0

    def add(self, name, highway, coords):
        minor = highway in MINOR_WAYS
        name_tokens = tokens(name)
        points = [to_xy(lon, lat) for lon, lat in coords]
        for (x1, y1), (x2, y2) in zip(points, points[1:]):
            segment = (x1, y1, x2, y2, name, name_tokens, minor)
            cx1, cx2 = sorted((int(x1 // CELL_METERS), int(x2 // CELL_METERS)))
            cy1, cy2 = sorted((int(y1 // CELL_METERS), int(y2 // CELL_METERS)))
            for cx in range(cx1, cx2 + 1):
                for cy in range(cy1, cy2 + 1):
                    self.cells[(cx, cy)].append(segment)
        self.count += 1

    def near(self, x, y):
        """Per ogni via entro MAX_METERS, la distanza del suo punto piu' vicino."""
        best = {}
        cx, cy = int(x // CELL_METERS), int(y // CELL_METERS)
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for x1, y1, x2, y2, name, name_tokens, minor in self.cells.get((cx + dx, cy + dy), ()):
                    distance = point_segment_distance(x, y, x1, y1, x2, y2)
                    if distance > MAX_METERS:
                        continue
                    known = best.get(name)
                    if known is None or distance < known[0]:
                        best[name] = (distance, name_tokens, minor)
        return best


def point_segment_distance(px, py, x1, y1, x2, y2):
    dx, dy = x2 - x1, y2 - y1
    length2 = dx * dx + dy * dy
    t = 0.0 if length2 == 0 else max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / length2))
    return math.hypot(px - (x1 + t * dx), py - (y1 + t * dy))


def pick_street(stop_name, candidates):
    """La via della fermata tra quelle vicine: (nome, metri, scelta per nome)."""
    if not candidates:
        return None
    roads = {n: c for n, c in candidates.items() if not c[2]}
    pool = roads or candidates
    # La prima parte del nome e' la via, la seconda la traversa.
    wanted = tokens(stop_name.split("/")[0])
    if wanted:
        matching = [
            (c[0], n) for n, c in pool.items()
            if c[0] <= NAME_MATCH_METERS and wanted <= c[1]
        ]
        if matching:
            distance, name = min(matching)
            return name, distance, True
    distance, name = min((c[0], n) for n, c in pool.items())
    return name, distance, False


class Area:
    """Un comune: tutti gli anelli insieme, con la regola pari/dispari (i buchi tornano da se')."""

    def __init__(self, name, geometry):
        self.name = name
        polygons = geometry["coordinates"] if geometry["type"] == "MultiPolygon" else [geometry["coordinates"]]
        self.edges = []
        for polygon in polygons:
            for ring in polygon:
                for (x1, y1), (x2, y2) in zip(ring, ring[1:]):
                    if y1 != y2:
                        self.edges.append((x1, y1, x2, y2))
        ys = [y for e in self.edges for y in (e[1], e[3])]
        xs = [x for e in self.edges for x in (e[0], e[2])]
        self.bbox = (min(xs), min(ys), max(xs), max(ys)) if self.edges else None

    def contains(self, lon, lat):
        if not self.bbox or not (self.bbox[0] <= lon <= self.bbox[2] and self.bbox[1] <= lat <= self.bbox[3]):
            return False
        inside = False
        for x1, y1, x2, y2 in self.edges:
            if (y1 > lat) != (y2 > lat) and lon < x1 + (lat - y1) * (x2 - x1) / (y2 - y1):
                inside = not inside
        return inside


def read_stops(gtfs):
    path = Path(gtfs)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            text = archive.read("stops.txt").decode("utf-8-sig")
    else:
        text = path.read_text(encoding="utf-8-sig")
    for row in csv.DictReader(io.StringIO(text)):
        try:
            yield row["stop_id"].strip(), row.get("stop_name", "").strip(), float(row["stop_lat"]), float(row["stop_lon"])
        except (KeyError, ValueError):
            continue


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pbf", required=True)
    parser.add_argument("--gtfs", required=True)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--boundary", default=str(DEFAULT_BOUNDARY))
    args = parser.parse_args()

    streets = StreetIndex()
    areas = []
    with tempfile.TemporaryDirectory() as tmp:
        wanted = ",".join(f"'{h}'" for h in sorted(ROADS | MINOR_WAYS))
        for feature in export_layer(args.pbf, "lines", f"name IS NOT NULL AND highway IN ({wanted})",
                                    "name,highway", Path(tmp) / "streets.geojsonl"):
            geometry = feature["geometry"]
            lines = geometry["coordinates"] if geometry["type"] == "MultiLineString" else [geometry["coordinates"]]
            for line in lines:
                streets.add(feature["properties"]["name"], feature["properties"]["highway"], line)
        # I comuni interi dentro l'estratto. Roma no: il suo confine esce
        # dall'estratto e GDAL non riesce a chiuderlo, quindi arriva da file.
        for feature in export_layer(args.pbf, "multipolygons", "boundary = 'administrative' AND admin_level = '8'",
                                    "name,admin_level", Path(tmp) / "areas.geojsonl"):
            if feature["properties"].get("name"):
                areas.append(Area(feature["properties"]["name"], feature["geometry"]))
    with open(args.boundary, encoding="utf-8") as handle:
        rome = json.load(handle)
    areas.append(Area(rome["properties"]["name"], rome["geometry"]))

    rows, by_name, no_street, no_city = [], 0, 0, 0
    for stop_id, stop_name, lat, lon in sorted(read_stops(args.gtfs)):
        picked = pick_street(stop_name, streets.near(*to_xy(lon, lat)))
        if picked is None:
            no_street += 1
            continue
        street, _, matched = picked
        by_name += matched
        city = next((area.name for area in areas if area.contains(lon, lat)), "")
        no_city += not city
        rows.append((stop_id, f"{lat:.6f}", f"{lon:.6f}", street, city))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("stop_id", "lat", "lon", "street", "city"))
        writer.writerows(rows)

    total = len(rows) + no_street
    print(f"vie con nome nell'estratto: {streets.count}", file=sys.stderr)
    print(f"fermate nel feed:           {total}", file=sys.stderr)
    print(f"  con la via:               {len(rows)} ({by_name} scelte per nome)", file=sys.stderr)
    print(f"  senza via entro {MAX_METERS:.0f} m:    {no_street}", file=sys.stderr)
    print(f"  con la via ma senza comune: {no_city}", file=sys.stderr)
    print(f"scritto {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
