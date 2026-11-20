"""Shared helpers for flat scraper listings.

Every scraper returns a list of plain dictionaries (no envelope wrapper). DuckDB
enforces the final schema in the ingestion phase; this module only keeps the
shape and types consistent between scrapers.
"""

import re
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


# Work-mode signals. ``work_mode`` is remote ONLY when the offer is fully
# remote; "remote possible" next to an office / hybrid option is hybrid.
_REMOTE_MARKERS = ("zdaln", "remote", "telecommute", "home office", "home-office")
_HYBRID_MARKERS = ("hybryd", "hybrid")
_OFFICE_MARKERS = ("stacjonar", "office", "on-site", "onsite", "on site", "siedzib", "w biurze")
# Phrases that mean "remote is possible / partial", not "fully remote".
_PARTIAL_REMOTE_MARKERS = (
    "możliwa praca zdalna",
    "możliwość pracy zdalnej",
    "częściowo zdaln",
    "remote possible",
    "partially remote",
    "partly remote",
    "remote optional",
    "occasional remote",
)
WORK_MODES = ("remote", "hybrid", "office")


def normalize_work_mode(value: Any) -> str | None:
    """Map free-text work modes to remote / hybrid / office.

    ``remote`` means FULLY remote. Rules, in order:
    - an explicit hybrid marker -> hybrid;
    - "remote possible" style phrases -> hybrid;
    - remote together with an office/on-site option -> hybrid;
    - remote alone -> remote; office alone -> office.
    Lists are joined, so ``["praca stacjonarna", "praca zdalna"]`` -> hybrid.
    """
    if isinstance(value, (list, tuple, set)):
        value = ", ".join(_text(item) for item in value if _text(item))
    text = _text(value).lower()
    if not text:
        return None
    if text in WORK_MODES:
        return text
    if any(marker in text for marker in _HYBRID_MARKERS):
        return "hybrid"
    if any(marker in text for marker in _PARTIAL_REMOTE_MARKERS):
        return "hybrid"
    remote = any(marker in text for marker in _REMOTE_MARKERS)
    office_text = text.replace("home office", "").replace("home-office", "")
    office = any(marker in office_text for marker in _OFFICE_MARKERS)
    if remote and office:
        return "hybrid"
    if remote:
        return "remote"
    if office:
        return "office"
    return None


# Canonical seniority levels, in order. ``seniority_levels`` is always an
# ordered subset of these.
SENIORITY_LEVELS = ("intern", "junior", "mid", "senior", "lead", "expert", "manager")

# (level, markers) checked in this order for every part of the board's text;
# the first match wins, so "młodszy specjalista" is junior and
# "starszy specjalista" senior before the generic "specjalista" -> mid.
_SENIORITY_RULES = (
    ("intern", ("intern", "trainee", "staż", "staz", "praktyk", "apprentice")),
    ("junior", ("junior", "młodsz", "mlodsz", "asystent", "assistant", "entry")),
    ("senior", ("senior", "starszy", "starsza")),
    ("lead", ("lead", "lider", "leader")),
    ("expert", ("expert", "ekspert", "principal", "master", "architect")),
    (
        "manager",
        (
            "manager", "menedżer", "menedzer", "kierowni", "koordynator",
            "head", "director", "dyrektor", "c_level", "c-level", "clevel",
            "executive", "prezes", "cto", "ceo",
        ),
    ),
    ("mid", ("mid", "regular", "medium", "intermediate", "specjalista", "specialist")),
)


def normalize_seniority_levels(value: Any) -> list[str] | None:
    """Board seniority text (or list) -> ordered canonical levels, None if unknown.

    "Mid, Senior" -> ["mid", "senior"]; "specjalista (Mid / Regular)" -> ["mid"];
    "Trainee" / "praktykant / stażysta" -> ["intern"]; "c_level" -> ["manager"].
    """
    if value is None:
        return None
    items = value if isinstance(value, (list, tuple, set)) else [value]
    found = set()
    for item in items:
        for part in re.split(r"[,;/|+&()→]|\band\b|\boraz\b|\blub\b|\bor\b", _text(item).lower()):
            part = part.strip()
            if not part:
                continue
            for level, markers in _SENIORITY_RULES:
                if any(marker in part for marker in markers):
                    found.add(level)
                    break
    levels = [level for level in SENIORITY_LEVELS if level in found]
    return levels or None


def tech_list_or_none(value: Any) -> list[str] | None:
    """Typed technology list: None stays None (board does not distinguish)."""
    if value is None:
        return None
    return text_list(value)


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
    technologies_required: Any = None,
    technologies_nice: Any = None,
    salary_month: Any = None,
    salary_hour: Any = None,
    currency: Any = None,
    salary_type: Any = None,
    employment_types: Iterable = (),
    seniority: Any = None,
    work_mode: Any = None,
    work_mode_raw: Any = None,
    seniority_levels: Any = None,
    application_url: Any = None,
    benefits: Iterable = (),
    date: Any = None,
    **extra: Any,
) -> dict:
    """Build one flat, JSON-serialisable job listing.

    ``technologies_required`` / ``technologies_nice``: pass None when the board
    does not distinguish required from nice-to-have, a list (possibly empty)
    when it does. ``technologies`` becomes the de-duplicated union of the
    untyped tags, the required and the nice-to-have technologies.
    ``seniority`` keeps the board's text; ``seniority_levels`` defaults to the
    normalised form of it. ``work_mode_raw`` is the board's original
    work-mode/location text, kept for auditing.
    """
    required = tech_list_or_none(technologies_required)
    nice = tech_list_or_none(technologies_nice)
    tech_list = text_list(text_list(technologies) + (required or []) + (nice or []))
    if seniority_levels is None:
        levels = normalize_seniority_levels(seniority)
    else:
        levels = normalize_seniority_levels(seniority_levels)
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
        "technologies_required": required,
        "technologies_nice": nice,
        "salary-month": int_or_none(salary_month),
        "salary-hour": int_or_none(salary_hour),
        "currency": str_or_none(currency),
        "salary_type": normalize_salary_type(salary_type),
        "employment_types": as_list(employment_types),
        "seniority": str_or_none(seniority),
        "seniority_levels": levels,
        "work_mode": normalize_work_mode(work_mode),
        "work_mode_raw": str_or_none(work_mode_raw),
        "application_url": str_or_none(application_url),
        "benefits": as_list(benefits),
        "date": str_or_none(date),
        "scraped_at": utc_now_iso(),
    }
    listing.update(extra)
    return listing
