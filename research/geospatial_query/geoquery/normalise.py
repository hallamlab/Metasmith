"""One AOI table (KML or GeoJSON) in, normalised AOI GeoJSON out."""
import argparse, json, re, unicodedata
import geopandas as gpd
import pandas as pd

NE_COUNTRIES = "https://naciscdn.org/naturalearth/50m/cultural/ne_50m_admin_0_countries.zip"


def slug(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def normalise(path):
    # a KML folder is a layer and the default one can be empty, so read every layer
    layers = [gpd.read_file(path, layer=n).to_crs(4326) for n in gpd.list_layers(path).name]
    g = pd.concat(layers, ignore_index=True)
    g = g[g.geometry.notna() & ~g.geometry.is_empty].reset_index(drop=True)
    if g.empty:
        raise SystemExit(f"no AOI geometries in {path}")
    g["geometry"] = g.geometry.make_valid()
    name = next((c for c in ("aoi_id", "Name", "name") if c in g), None)
    g["aoi_name"] = g[name].astype(str) if name else [f"aoi_{i + 1}" for i in range(len(g))]
    g["aoi_id"] = g["aoi_name"].map(slug)
    if g["aoi_id"].duplicated().any():
        raise SystemExit(f"duplicate aoi ids: {sorted(g.aoi_id[g.aoi_id.duplicated()])}")
    g["area_km2"] = (g.to_crs("+proj=cea").area / 1e6).round(3)
    if "country" not in g:
        ne = gpd.read_file(NE_COUNTRIES)[["ISO_A2_EH", "geometry"]].to_crs(4326)
        pts = gpd.GeoDataFrame(geometry=g.geometry.representative_point(), crs=4326)
        g["country"] = gpd.sjoin(pts, ne, how="left", predicate="within")["ISO_A2_EH"] \
            .groupby(level=0).first().fillna("").values
    if "aoi_class" not in g:
        g["aoi_class"] = ""
    return g[["aoi_id", "aoi_name", "aoi_class", "country", "area_km2", "geometry"]]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in", dest="src", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    g = normalise(a.src)
    with open(a.out, "w") as fh:
        json.dump(json.loads(g.to_json(drop_id=True)), fh, indent=1)
    print(f"{len(g)} AOIs -> {a.out}")


if __name__ == "__main__":
    main()
