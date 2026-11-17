"""Shared helpers for flat scraper listings.

Every scraper returns a list of plain dictionaries (no envelope wrapper). DuckDB
enforces the final schema in the ingestion phase; this module only keeps the
shape and types consistent between scrapers.
"""

from datetime import datetime, timezone
from typing import Any, Iterable


def utc_now_iso() -> str:
    """Current UTC timestamp, ISO-8601, second precision."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _text(value: Any) -> str:
    return str(value or "").strip()


def as_list(value: Any) -> list[str]:
    """Normalize a scalar / iterable into a clean list of non-empty strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in value:
            text = _text(item)
            if text and text not in out:
                out.append(text)
        return out
    text = _text(value)
    return [text] if text else []


def text_list(value: Any) -> list[str]:
    """Like as_list, but also splits comma-separated strings (including list items)."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_parts = []
        for item in value:
            raw_parts.extend(str(item or "").split(","))
    else:
        raw_parts = str(value).split(",")
    out = []
    for part in raw_parts:
        text = part.strip()
        if text and text not in out:
            out.append(text)
    return out


def int_or_none(value: Any) -> int | None:
    """Salary values: 0 / blank / unparsable mean 'not disclosed' -> None."""
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return int(round(number))


def str_or_none(value: Any) -> str | None:
    text = _text(value)
    return text or None


def normalize_salary_type(value: Any) -> str | None:
    """Map free-text salary descriptions to 'brutto' / 'netto'."""
    text = _text(value).lower()
    if "netto" in text:
        return "netto"
    if "brutto" in text:
        return "brutto"
    # Polish convention: B2B rates are net (+ VAT); UoP is gross.
    if "b2b" in text:
        return "netto"
    if "uop" in text or "umowa o pracę" in text:
        return "brutto"
    return None


def normalize_work_mode(value: Any) -> str | None:
    """Map free-text work modes to remote / hybrid / office."""
    text = _text(value).lower()
    if not text:
        return None
    if "zdaln" in text or "remote" in text:
        return "remote"
    if "hybryd" in text or "hybrid" in text:
        return "hybrid"
    if "stacjonar" in text or "office" in text or "siedzib" in text:
        return "office"
    return None


def flat_listing(
    *,
    board: str,
    url: str = "",
    company: str = "",
    offer_id: Any = "",
    title: str = "",
    city: str = "",
    description: str = "",
    technologies: Any = None,
    salary_month: Any = None,
    salary_hour: Any = None,
    currency: Any = None,
    salary_type: Any = None,
    employment_types: Iterable = (),
    seniority: Any = None,
    work_mode: Any = None,
    application_url: Any = None,
    benefits: Iterable = (),
    date: Any = None,
    **extra: Any,
) -> dict:
    """Build one flat, JSON-serialisable job listing."""
    tech_list = text_list(technologies)
    listing = {
        "board": _text(board),
        "url": _text(url),
        "company": _text(company),
        "offer_id": _text(offer_id),
        "title": _text(title),
        "city": _text(city),
        "description": str(description or ""),
        "technologies": ", ".join(tech_list),
        "technologies_list": tech_list,
        "salary-month": int_or_none(salary_month),
        "salary-hour": int_or_none(salary_hour),
        "currency": str_or_none(currency),
        "salary_type": normalize_salary_type(salary_type),
        "employment_types": as_list(employment_types),
        "seniority": str_or_none(seniority),
        "work_mode": normalize_work_mode(work_mode),
        "application_url": str_or_none(application_url),
        "benefits": as_list(benefits),
        "date": str_or_none(date),
        "scraped_at": utc_now_iso(),
    }
    listing.update(extra)
    return listing
