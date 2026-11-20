"""Progress hooks for scrapers.

Scrapers call the module-level functions (``stage``, ``advance`` ...) without
knowing who is listening. The hub ``bind``s each scraper run to its name (a
context variable, so it follows asyncio tasks and ``asyncio.to_thread``), and a
front end such as ``dashboard.Dashboard`` installs a sink that receives
``(scraper, kind, payload)`` events.

With no sink installed every call is a cheap no-op, so scrapers still run
stand-alone (``python -m scrapers.pracuj.pracuj``) exactly as before.

Scrapers running in a child process (ProcessPoolExecutor) get a sink that puts
events on a multiprocessing queue; ``start_forwarder`` replays them in the
parent. Their stdout / stderr travel the same way, as ``log`` events.
"""

import contextvars
import sys
import threading
from typing import Any, Callable

Sink = Callable[[str | None, str, tuple], None]

_current: contextvars.ContextVar[str | None] = contextvars.ContextVar("scraper", default=None)
_sink: Sink | None = None


# --------------------------------------------------------------------------- #
# Wiring (hub / front end)
# --------------------------------------------------------------------------- #
def install(sink: Sink | None) -> None:
    """Route events to ``sink`` (None switches reporting off)."""
    global _sink
    _sink = sink


def active() -> bool:
    return _sink is not None


def bind(name: str) -> None:
    """Attribute everything in the current task / thread context to ``name``."""
    _current.set(name)


def current() -> str | None:
    return _current.get()


def _emit(kind: str, *payload: Any) -> None:
    sink = _sink
    if sink is None:
        return
    name = _current.get()
    if name is None:
        return
    try:
        sink(name, kind, payload)
    except Exception:
        pass  # progress reporting must never break a scrape


# --------------------------------------------------------------------------- #
# Scraper-facing API
# --------------------------------------------------------------------------- #
def stage(label: str, total: int | None = None) -> None:
    """Start a new phase: resets the bar to 0 (``total=None`` = unknown size)."""
    _emit("stage", label, total)


def set_total(total: int | None) -> None:
    _emit("total", total)


def add_total(n: int = 1) -> None:
    """Grow the current phase's total (work discovered while running)."""
    _emit("add_total", n)


def advance(n: int = 1) -> None:
    _emit("advance", n)


def label(text: str) -> None:
    """Rename the current phase without resetting its counters."""
    _emit("label", text)


def set_state(state: str, **info: Any) -> None:
    """Lifecycle: running / done / failed. Falls back to a print without a sink."""
    if _sink is None:
        name = _current.get() or "?"
        extra = f" ({info['error']})" if info.get("error") else ""
        print(f"[{name}] {state}{extra}")
        return
    _emit("state", state, info)


# --------------------------------------------------------------------------- #
# Child processes
# --------------------------------------------------------------------------- #
class _QueueWriter:
    """File-like object that sends complete lines to the parent as log events."""

    def __init__(self, queue, is_err: bool):
        self._queue = queue
        self._is_err = is_err
        self._buf = ""
        self._lock = threading.Lock()

    def write(self, text: str) -> int:
        with self._lock:
            self._buf += text
            *lines, self._buf = self._buf.split("\n")
        for line in lines:
            self._queue.put((_current.get(), "log", (line, self._is_err)))
        return len(text)

    def flush(self) -> None:
        with self._lock:
            line, self._buf = self._buf, ""
        if line:
            self._queue.put((_current.get(), "log", (line, self._is_err)))

    def isatty(self) -> bool:
        return False


def init_child(queue) -> None:
    """ProcessPoolExecutor initializer: forward events and output to ``queue``."""
    if queue is None:
        return
    install(lambda name, kind, payload: queue.put((name, kind, payload)))
    sys.stdout = _QueueWriter(queue, is_err=False)
    sys.stderr = _QueueWriter(queue, is_err=True)


def start_forwarder(queue) -> threading.Thread:
    """Replay child-process events into this process's sink until ``None`` arrives."""

    def run():
        while True:
            item = queue.get()
            if item is None:
                return
            name, kind, payload = item
            sink = _sink
            if sink is not None and name is not None:
                try:
                    sink(name, kind, payload)
                except Exception:
                    pass

    thread = threading.Thread(target=run, name="progress-forwarder", daemon=True)
    thread.start()
    return thread
