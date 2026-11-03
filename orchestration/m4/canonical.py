"""Versioned typed canonical row stream; engines must sort by the same unique key.

Length framing prevents null/string/separator ambiguity. Unicode is UTF-8 without
normalization. Floats use exact binary64 network-order bits; NaN/Inf are rejected.
Cross-engine marts deliberately use BIGINT sufficient statistics, not floats.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import struct

ENCODING = "m4-typed-framed-v1"


def frame(data):
    return str(len(data)).encode("ascii") + b":" + data


def cell(value, dtype):
    if value is None:
        return b"N"
    if dtype in {"BIGINT", "INTEGER", "HUGEINT", "SMALLINT", "TINYINT"}:
        if isinstance(value, bool) or int(value) != value:
            raise ValueError("noninteger in integer column")
        result = str(int(value)).encode("ascii")
    elif dtype == "VARCHAR":
        if not isinstance(value, str):
            raise ValueError("string required")
        result = value.encode("utf-8")
    elif dtype == "DOUBLE":
        if not math.isfinite(float(value)):
            raise ValueError("nonfinite fact value")
        result = struct.pack("!d", float(value))
    elif dtype.startswith("DECIMAL("):
        precision, scale = map(int, dtype[8:-1].split(","))
        decimal = Decimal(value)
        quantum = Decimal(1).scaleb(-scale)
        quantized = decimal.quantize(quantum)
        if not decimal.is_finite() or quantized != decimal or len(quantized.as_tuple().digits) > precision:
            raise ValueError("decimal violates declared exact precision/scale")
        result = format(quantized, f".{scale}f").encode("ascii")
    elif dtype == "DATE":
        if isinstance(value, datetime):
            raise ValueError("datetime in date column")
        result = (value if isinstance(value, date) else date.fromisoformat(value)).isoformat().encode("ascii")
    else:
        raise ValueError(f"unhandled exact type {dtype}")
    return b"V" + frame(result)


def row_bytes(row, columns):
    if len(row) != len(columns):
        raise ValueError("row/schema length mismatch")
    return b"R" + b"".join(frame(cell(value, dtype)) for value, (_, dtype) in zip(row, columns)) + b"\n"


def digest_sorted(rows, columns, key_columns):
    """Bounded memory for SQL ORDER BY cursor iteration; reject duplicate/null keys."""
    positions = [next(i for i, col in enumerate(columns) if col[0] == name) for name in key_columns]
    digest = hashlib.sha256()
    digest.update(ENCODING.encode("ascii") + b"\n")
    digest.update(json.dumps(columns, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n")
    previous = None
    nrows = 0
    for row in rows:
        grain = tuple(row[i] for i in positions)
        if any(v is None for v in grain) or (previous is not None and grain <= previous):
            raise ValueError("nonunique, null or unsorted unique key")
        previous = grain
        digest.update(row_bytes(row, columns))
        nrows += 1
    return {"encoding": ENCODING, "columns": columns, "key": key_columns,
            "rows": nrows, "sha256": digest.hexdigest()}


def digest_small(rows, columns, key_columns):
    """Only for small tests; production sorting is performed by the SQL engine."""
    positions = [next(i for i, c in enumerate(columns) if c[0] == name) for name in key_columns]
    return digest_sorted(sorted(rows, key=lambda row: tuple(row[i] for i in positions)), columns, key_columns)
