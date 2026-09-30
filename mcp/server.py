#!/usr/bin/env python3
"""Thin stdio MCP adapter for the local Google Maps Scraper REST API.

The scraper itself stays untouched and runs on http://127.0.0.1:8080.
This adapter exposes short, job-oriented tools so an MCP client does not
need to keep one long scrape request open.
"""

from __future__ import annotations

import csv
import io
import json
import os
import urllib.error
import urllib.request
from typing import Any

from mcp.server.fastmcp import FastMCP

API_BASE = os.environ.get("GMAPS_API_URL", "http://127.0.0.1:8080").rstrip("/")
mcp = FastMCP("google-maps-scraper")


def _json_request(path: str, method: str = "GET", payload: dict[str, Any] | None = None) -> Any:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(f"{API_BASE}{path}", data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if not raw:
                return {"ok": True, "status": resp.status}
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                return json.loads(raw.decode("utf-8"))
            return raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Google Maps Scraper API returned HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Cannot reach Google Maps Scraper at {API_BASE}. "
            "Start the local scraper web API first."
        ) from exc


@mcp.tool()
def health() -> dict[str, Any]:
    """Check whether the local Google Maps Scraper REST API is reachable."""
    jobs = _json_request("/api/v1/jobs")
    return {"ok": True, "api_base": API_BASE, "jobs_visible": len(jobs) if isinstance(jobs, list) else None}


@mcp.tool()
def start_scrape(
    keywords: list[str],
    name: str = "ChatGPT scrape",
    lang: str = "en",
    depth: int = 1,
    email: bool = False,
    max_time_seconds: int = 300,
    radius_meters: int = 10000,
    zoom: int = 15,
    fast_mode: bool = False,
    latitude: str = "",
    longitude: str = "",
    proxies: list[str] | None = None,
) -> dict[str, Any]:
    """Start a Google Maps scraping job and return its job id.

    Use get_job(job_id) to check progress and get_results(job_id) after
    status becomes "ok". For fast_mode, latitude and longitude are required.
    """
    clean_keywords = [k.strip() for k in keywords if k and k.strip()]
    if not clean_keywords:
        raise ValueError("At least one non-empty keyword is required.")
    if len(lang) != 2:
        raise ValueError("lang must be a 2-letter language code.")
    if depth < 1:
        raise ValueError("depth must be >= 1.")
    if max_time_seconds < 180:
        max_time_seconds = 180
    if fast_mode and (not latitude or not longitude):
        raise ValueError("fast_mode requires latitude and longitude.")

    payload = {
        "Name": name,
        "keywords": clean_keywords,
        "lang": lang,
        "zoom": zoom,
        "lat": latitude,
        "lon": longitude,
        "fast_mode": fast_mode,
        "radius": radius_meters,
        "depth": depth,
        "email": email,
        "extra_reviews": False,
        "max_time": max_time_seconds,
        "proxies": proxies or [],
    }
    result = _json_request("/api/v1/jobs", "POST", payload)
    return {"started": True, "job": result, "next": "Call get_job with the returned id."}


@mcp.tool()
def list_jobs() -> Any:
    """List local Google Maps scraping jobs and their statuses."""
    return _json_request("/api/v1/jobs")


@mcp.tool()
def get_job(job_id: str) -> Any:
    """Get one Google Maps scraping job, including pending/working/ok/failed status."""
    return _json_request(f"/api/v1/jobs/{job_id}")


@mcp.tool()
def get_results(job_id: str, max_rows: int = 500) -> dict[str, Any]:
    """Download completed CSV results and return structured lead rows.

    max_rows limits how much data is returned to the model in one call.
    """
    if max_rows < 1:
        raise ValueError("max_rows must be >= 1.")
    max_rows = min(max_rows, 5000)

    job = _json_request(f"/api/v1/jobs/{job_id}")
    if isinstance(job, dict) and job.get("Status") not in (None, "ok") and job.get("status") not in (None, "ok"):
        status = job.get("Status", job.get("status"))
        return {"ready": False, "status": status, "job": job}

    req = urllib.request.Request(f"{API_BASE}/api/v1/jobs/{job_id}/download", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            text = resp.read().decode("utf-8-sig", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Could not download job {job_id}: HTTP {exc.code}: {detail}") from exc

    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for i, row in enumerate(reader):
        if i >= max_rows:
            break
        rows.append(dict(row))

    return {
        "ready": True,
        "job_id": job_id,
        "returned_rows": len(rows),
        "max_rows": max_rows,
        "rows": rows,
    }


if __name__ == "__main__":
    mcp.run()
