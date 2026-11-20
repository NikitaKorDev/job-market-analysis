"""Live terminal dashboard for the scrape run.

One row per scraper: status, progress bar, counts, rate, ETA, elapsed time and
how long the scraper has been silent. A scraper is *quiet* after
``quiet_after`` seconds without output or progress, and *stalled* after
``stall_after`` seconds; stalls (and recoveries) are also logged as lines, so
they show up in non-interactive logs too.

Scraper output is captured (stdout/stderr, attributed per scraper through
``scrapers.progress``) and printed above the live bars with a coloured tag.
"""

import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field

from rich.console import Console, Group
from rich.live import Live
from rich.progress_bar import ProgressBar
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from scrapers import progress

TAG_COLORS = ("cyan", "magenta", "green", "yellow", "blue", "bright_red", "bright_cyan", "bright_magenta")
SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
NOTABLE_WORDS = ("!", "error", "fail", "warn", "traceback", "exception", "blocked", "incomplete")
RATE_WINDOW_S = 30.0


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "--:--"
    seconds = int(max(seconds, 0))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _fmt_short(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    return f"{m}m{s:02d}s" if m < 60 else f"{m // 60}h{m % 60:02d}m"


@dataclass
class Row:
    name: str
    color: str
    state: str = "waiting"  # waiting / running / done / failed
    stage: str = ""
    total: int | None = None
    completed: int = 0
    started: float | None = None
    ended: float | None = None
    last_activity: float = field(default_factory=time.monotonic)
    last_line: str = ""
    count: int | None = None
    error: str = ""
    stalled: bool = False
    longest_silence: float = 0.0
    stall_events: int = 0
    samples: deque = field(default_factory=lambda: deque(maxlen=512))

    def silence(self, now: float) -> float:
        return now - self.last_activity

    def rate(self, now: float) -> float | None:
        """Items/second over the last RATE_WINDOW_S seconds of this stage."""
        while self.samples and now - self.samples[0][0] > RATE_WINDOW_S:
            self.samples.popleft()
        if len(self.samples) < 2:
            return None
        (t0, c0), (t1, c1) = self.samples[0], self.samples[-1]
        if t1 - t0 < 1.0:
            return None
        # Measure up to "now" so the rate decays while nothing is happening.
        return (c1 - c0) / (now - t0)


class Dashboard:
    def __init__(
        self,
        scrapers: list[str],
        quiet_after: float = 30.0,
        stall_after: float = 120.0,
        show_logs: str = "all",  # all / notable / none
        console: Console | None = None,
    ):
        self.console = console or Console(file=sys.__stdout__, stderr=False)
        self.quiet_after = quiet_after
        self.stall_after = stall_after
        self.show_logs = show_logs
        self.rows = {
            name: Row(name, TAG_COLORS[i % len(TAG_COLORS)]) for i, name in enumerate(scrapers)
        }
        self.name_width = max((len(n) for n in scrapers), default=8)
        self.started = time.monotonic()
        self._lock = threading.RLock()
        self._live: Live | None = None
        self._watchdog: threading.Thread | None = None
        self._stop = threading.Event()
        self._saved_streams = None
        # Lines to print once self._lock is released. Never print while holding
        # self._lock: Live's refresh thread holds the console lock while it
        # renders (and needs self._lock), so the opposite order would deadlock.
        self._outbox: list[Text] = []
        self._bar_width = 28

    # ------------------------------------------------------------------ lifecycle
    def __enter__(self):
        self._saved_streams = (sys.stdout, sys.stderr)
        self._live = Live(
            self,
            console=self.console,
            refresh_per_second=8,
            transient=False,
            redirect_stdout=False,
            redirect_stderr=False,
        )
        self._live.__enter__()
        sys.stdout = _Router(self, is_err=False)
        sys.stderr = _Router(self, is_err=True)
        progress.install(self.handle)
        self._watchdog = threading.Thread(target=self._watch, name="stall-watchdog", daemon=True)
        self._watchdog.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        progress.install(None)
        for stream in (sys.stdout, sys.stderr):
            if isinstance(stream, _Router):
                stream.drain()
        self._flush_outbox()
        sys.stdout, sys.stderr = self._saved_streams
        if self._live is not None:
            self._live.__exit__(*exc)
        return False

    # ------------------------------------------------------------------ events
    def handle(self, name: str, kind: str, payload: tuple) -> None:
        row = self.rows.get(name)
        if row is None:
            return
        now = time.monotonic()
        try:
            with self._lock:
                self._apply(row, kind, payload, now)
        finally:
            self._flush_outbox()

    def _apply(self, row: Row, kind: str, payload: tuple, now: float) -> None:
        self._touch(row, now)
        if kind == "log":
            line, is_err = payload
            if line.strip():
                row.last_line = line.strip()
            self._print_log(row, line, is_err)
        elif kind == "stage":
            row.stage, row.total = payload
            row.completed = 0
            row.samples.clear()
            row.samples.append((now, 0))
        elif kind == "total":
            (row.total,) = payload
        elif kind == "add_total":
            row.total = (row.total or 0) + payload[0]
        elif kind == "advance":
            row.completed += payload[0]
            row.samples.append((now, row.completed))
        elif kind == "label":
            (row.stage,) = payload
        elif kind == "state":
            state, info = payload
            self._set_state(row, state, info, now)

    def _touch(self, row: Row, now: float) -> None:
        silence = row.silence(now)
        row.longest_silence = max(row.longest_silence, silence) if row.state == "running" else row.longest_silence
        if row.stalled:
            row.stalled = False
            self._print_event(f"↻ {row.name} resumed after {_fmt_short(silence)} of silence", "green")
        row.last_activity = now

    def _set_state(self, row: Row, state: str, info: dict, now: float) -> None:
        row.state = state
        if state == "running":
            row.started = row.started or now
            row.stage = row.stage or "starting"
        elif state in ("done", "failed"):
            row.ended = now
            row.count = info.get("count")
            row.error = info.get("error") or ""
            if state == "done":
                self._print_event(
                    f"✔ {row.name} finished in {fmt_duration(now - (row.started or now))}: "
                    f"{row.count or 0:,} listings",
                    "green",
                )
            else:
                self._print_event(f"✖ {row.name} failed: {row.error}", "bold red")

    def _watch(self) -> None:
        """Flag scrapers that went silent; log it once per stall."""
        while not self._stop.wait(1.0):
            now = time.monotonic()
            self._flush_outbox()
            with self._lock:
                for row in self.rows.values():
                    if row.state != "running" or row.stalled:
                        continue
                    silence = row.silence(now)
                    if silence >= self.stall_after:
                        row.stalled = True
                        row.stall_events += 1
                        last = f" — last output: {row.last_line[:120]!r}" if row.last_line else ""
                        self._print_event(
                            f"⚠ {row.name} looks STALLED: no output or progress for "
                            f"{_fmt_short(silence)} (stage: {row.stage or '?'}){last}",
                            "bold red",
                        )
            self._flush_outbox()

    # ------------------------------------------------------------------ output
    def _flush_outbox(self) -> None:
        with self._lock:
            pending, self._outbox = self._outbox, []
        for text in pending:
            self.console.print(text, highlight=False)

    def _print_log(self, row: Row | None, line: str, is_err: bool) -> None:
        if self.show_logs == "none":
            return
        notable = is_err or any(word in line.lower() for word in NOTABLE_WORDS)
        if self.show_logs == "notable" and not notable:
            return
        text = Text()
        text.append(time.strftime("%H:%M:%S "), style="dim")
        if row is not None:
            text.append(f"{row.name:<{self.name_width}}", style=f"bold {row.color}")
            text.append(" │ ", style="dim")
        text.append(line.rstrip(), style="red" if is_err else ("yellow" if notable else ""))
        self._outbox.append(text)

    def _print_event(self, message: str, style: str) -> None:
        text = Text()
        text.append(time.strftime("%H:%M:%S "), style="dim")
        text.append(message, style=style)
        self._outbox.append(text)

    def log_main(self, line: str, is_err: bool = False) -> None:
        """Output that isn't attributed to any scraper (main.py, hub)."""
        self._flush_outbox()  # keep ordering with already queued scraper lines
        self.console.print(Text(line.rstrip(), style="red" if is_err else ""), highlight=False)

    # ------------------------------------------------------------------ rendering
    def __rich__(self):
        now = time.monotonic()
        # Fixed columns + padding take ~70 cells besides name, bar and stage;
        # the bar shrinks on narrow terminals so names never get truncated.
        self._bar_width = max(10, min(32, self.console.width - self.name_width - 90))
        with self._lock:
            rows = list(self.rows.values())
            table = Table.grid(padding=(0, 1), expand=True)
            table.add_column(width=1, no_wrap=True)                       # status icon
            table.add_column(width=self.name_width, no_wrap=True)         # name
            table.add_column(width=self._bar_width, no_wrap=True)         # bar
            table.add_column(width=4, justify="right", no_wrap=True)      # percent
            table.add_column(width=15, justify="right", no_wrap=True)     # count
            table.add_column(width=7, justify="right", no_wrap=True)      # rate
            table.add_column(width=8, justify="right", no_wrap=True)      # eta
            table.add_column(width=7, justify="right", no_wrap=True)      # elapsed
            table.add_column(width=13, no_wrap=True)                      # activity
            table.add_column(ratio=1, no_wrap=True, overflow="ellipsis")  # stage
            table.add_row(
                *(Text(h, style="dim") for h in
                  ("", "scraper", "", "", "items", "rate", "eta", "elapsed", "activity", "stage"))
            )
            for row in rows:
                table.add_row(*self._render_row(row, now))

            running = sum(r.state == "running" for r in rows)
            stalled = sum(r.state == "running" and r.stalled for r in rows)
            done = sum(r.state == "done" for r in rows)
            failed = sum(r.state == "failed" for r in rows)
            listings = sum(r.count or 0 for r in rows)
            footer = Text()
            footer.append(f" ⏱ {fmt_duration(now - self.started)}", style="bold")
            footer.append(f"   ▶ {running} running", style="cyan")
            if stalled:
                footer.append(f"   ⚠ {stalled} stalled", style="bold red")
            footer.append(f"   ✔ {done} done", style="green")
            if failed:
                footer.append(f"   ✖ {failed} failed", style="red")
            footer.append(f"   ∑ {listings:,} listings", style="bold")
        return Group(Rule("scrapers", style="dim"), table, footer)

    def _render_row(self, row: Row, now: float):
        silence = row.silence(now)
        if row.state == "waiting":
            icon, color = Text("·", style="dim"), "dim"
        elif row.state == "done":
            icon, color = Text("✔", style="bold green"), "green"
        elif row.state == "failed":
            icon, color = Text("✖", style="bold red"), "red"
        elif row.stalled:
            icon, color = Text("⚠", style="bold red blink"), "red"
        elif silence >= self.quiet_after:
            icon, color = Text("…", style="bold yellow"), "yellow"
        else:
            frame = SPINNER[int(now * 10) % len(SPINNER)]
            icon, color = Text(frame, style=f"bold {row.color}"), row.color

        name = Text(row.name, style=f"bold {row.color}" if row.state != "waiting" else "dim")

        finished = row.state in ("done", "failed")
        known_total = row.total is not None and row.total > 0
        if finished:
            bar = ProgressBar(total=1, completed=1, width=self._bar_width,
                              complete_style=color, finished_style=color)
        elif row.state == "running" and not known_total:
            # A stalled scraper's pulse freezes, so it visibly stops moving.
            bar = ProgressBar(total=None, width=self._bar_width, pulse_style=color,
                              animation_time=row.last_activity if row.stalled else now)
        else:
            bar = ProgressBar(
                total=row.total or 1, completed=min(row.completed, row.total or 0), width=self._bar_width,
                complete_style=color, finished_style=color, style="grey23",
            )

        pct = Text("")
        if known_total and not finished:
            pct = Text(f"{min(row.completed / row.total, 1):.0%}", style=color)

        if finished:
            count = Text(f"{row.count or 0:,} jobs", style=color)
        elif known_total:
            count = Text(f"{row.completed:,}/{row.total:,}")
        elif row.completed:
            count = Text(f"{row.completed:,}/?")
        else:
            count = Text("")

        rate_value = row.rate(now) if row.state == "running" else None
        rate = Text(f"{rate_value:.1f}/s" if rate_value else "", style="dim")

        eta = Text("")
        if row.state == "running" and known_total and rate_value:
            remaining = max(row.total - row.completed, 0) / rate_value
            eta = Text(f"~{fmt_duration(remaining)}", style="dim")

        start = row.started
        elapsed = Text(fmt_duration((row.ended or now) - start) if start else "", style="dim")

        if row.state != "running":
            activity = Text("")
        elif row.stalled:
            activity = Text(f"STALL {_fmt_short(silence)}", style="bold white on red")
        elif silence >= self.quiet_after:
            activity = Text(f"idle {_fmt_short(silence)}", style="yellow")
        else:
            activity = Text("● live", style="green")

        if row.state == "failed":
            stage = Text(row.error, style="red")
        elif row.state == "done":
            stage = Text("done", style="dim")
        else:
            stage = Text(row.stage, style="dim")

        return icon, name, bar, pct, count, rate, eta, elapsed, activity, stage

    # ------------------------------------------------------------------ summary
    def summary_table(self) -> Table:
        table = Table(title="Scrape summary", title_style="bold", header_style="bold", box=None,
                      padding=(0, 2))
        table.add_column("scraper")
        table.add_column("status")
        table.add_column("listings", justify="right")
        table.add_column("time", justify="right")
        table.add_column("longest silence", justify="right")
        table.add_column("note")
        for row in self.rows.values():
            status = {
                "done": Text("✔ done", style="green"),
                "failed": Text("✖ failed", style="red"),
                "running": Text("… unfinished", style="yellow"),
                "waiting": Text("· not run", style="dim"),
            }[row.state]
            silence = row.longest_silence
            if row.state == "running":  # interrupted mid-run: include the current silence
                silence = max(silence, row.silence(time.monotonic()))
            silence_style = "red" if silence >= self.stall_after else (
                "yellow" if silence >= self.quiet_after else "dim")
            note = row.error or (f"stalled {row.stall_events}×" if row.stall_events else "")
            table.add_row(
                Text(row.name, style=f"bold {row.color}"),
                status,
                f"{row.count or 0:,}",
                fmt_duration((row.ended or time.monotonic()) - row.started) if row.started else "",
                Text(_fmt_short(silence) if row.started else "", style=silence_style),
                Text(note, style="red" if row.error or row.stall_events else ""),
            )
        return table


class _Router:
    """sys.stdout/stderr replacement: complete lines go to the dashboard,
    attributed to the scraper bound in the writer's context."""

    def __init__(self, dashboard: Dashboard, is_err: bool):
        self._dashboard = dashboard
        self._is_err = is_err
        self._buffers: dict[tuple, str] = {}
        self._lock = threading.Lock()

    def _key(self):
        return progress.current(), threading.get_ident()

    def write(self, text: str) -> int:
        key = self._key()
        with self._lock:
            buf = self._buffers.get(key, "") + text
            *lines, rest = buf.split("\n")
            self._buffers[key] = rest
        for line in lines:
            self._emit(key[0], line)
        return len(text)

    def _emit(self, name: str | None, line: str) -> None:
        if name is not None and name in self._dashboard.rows:
            self._dashboard.handle(name, "log", (line, self._is_err))
        else:
            self._dashboard.log_main(line, self._is_err)

    def flush(self) -> None:
        """No-op: emitting half a line here would split it in two."""

    def drain(self) -> None:
        """Emit any unterminated lines (called when the dashboard closes)."""
        with self._lock:
            pending, self._buffers = self._buffers, {}
        for (name, _), line in pending.items():
            if line:
                self._emit(name, line)

    def isatty(self) -> bool:
        return False

    @property
    def encoding(self) -> str:
        return "utf-8"
