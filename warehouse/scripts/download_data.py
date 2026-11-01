"""Download the six CSVs atomically and record their actual bytes, rows and hashes."""
import hashlib
import json
import time
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw"
BASE = "https://huggingface.co/datasets/attik/Instacart-Market-Basket-Analysis/resolve/main"
FILES = ("aisles", "departments", "products", "orders", "order_products__train", "order_products__prior")


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    records = []
    for name in FILES:
        path = RAW / f"{name}.csv"
        url = f"{BASE}/{name}.csv"
        if not path.is_file():
            partial = path.with_suffix(".csv.part")
            for attempt in range(3):
                try:
                    with urlopen(url, timeout=120) as response, partial.open("wb") as output:
                        while chunk := response.read(1024 * 1024):
                            output.write(chunk)
                    partial.replace(path)
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(2)
        digest = hashlib.sha256()
        newlines = 0
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
                newlines += chunk.count(b"\n")
        record = {"file": path.name, "url": url, "bytes": path.stat().st_size,
                  "data_rows": newlines - 1, "sha256": digest.hexdigest()}
        if record["data_rows"] < 1:
            raise RuntimeError(f"Empty dataset: {path}")
        records.append(record)
        print(json.dumps(record), flush=True)
    (ROOT / "results").mkdir(exist_ok=True)
    (ROOT / "results/data_manifest.json").write_text(json.dumps(records, indent=2) + "\n")


if __name__ == "__main__":
    main()
