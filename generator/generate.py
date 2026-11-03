#!/usr/bin/env python3
"""Fake log generator: one rotating log file per simulated service.

Odd-numbered services write JSON lines, even-numbered ones write plain text,
so the downstream parser has to handle both. A trace ID appears on roughly
30% of lines, the way it would in a real fleet where only request-handling
code carries a trace.
"""
import argparse
import datetime
import json
import logging
import random
import time
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Logs go in a "logs" folder next to this script unless --log-dir says otherwise.
DEFAULT_LOG_DIR = Path(__file__).resolve().parent / "logs"
DEFAULT_MAX_BYTES = 1_000_000   # rotate each file at about 1 MB
DEFAULT_BACKUP_COUNT = 3        # keep service-1.log.1 to .3; older copies are deleted

LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
LEVEL_WEIGHTS = [20, 60, 12, 6, 2]   # a healthy service is mostly INFO, rarely FATAL
STATUS_CODES = {
    "DEBUG": [200, 201, 204],
    "INFO": [200, 201, 204],
    "WARN": [400, 404, 429],
    "ERROR": [500, 502, 503],
    "FATAL": [500, 502, 503],
}

# Stage 12: which format each service logs in. Real fleets are mixed: newer
# services emit JSON, older ones emit plain text with a conventional prefix.
# Service i gets FORMATS[i % len(FORMATS)], so service-1 is JSON, service-2 text, ...
FORMATS = ["json", "text"]
HOSTS_PER_SERVICE = 3   # each service "runs" on a few hosts, named <service>-host-N

# Stage 13: a trace ID ties together every line written while handling one
# request. Background jobs, startup and health checks have no request, so
# most lines carry none.
TRACE_ID_PCT = 30

# Stage 13: message templates per level, so lines read like real logs and
# give the processor something to parse besides the fixed fields.
MESSAGES = {
    "DEBUG": [
        "cache lookup key={key}",
        "db query took {latency}ms",
        "retrying upstream call attempt={n}",
    ],
    "INFO": [
        "request completed path={path}",
        "user login ok user={user}",
        "order placed order={order}",
        "health check ok",
    ],
    "WARN": [
        "slow response path={path} latency={latency}ms",
        "rate limit near for user={user}",
        "retrying upstream call attempt={n}",
    ],
    "ERROR": [
        "payment declined order={order} reason=card_expired",
        "upstream timeout path={path}",
        "db connection lost",
    ],
    "FATAL": [
        "out of memory, shutting down",
        "config missing, cannot start",
    ],
}
PATHS = ["/api/orders", "/api/users", "/api/search", "/health", "/api/checkout"]

# Stage 9: the ways a line can be broken, per format.
JSON_MALFORMED_KINDS = ["truncate", "missing_field", "wrong_type", "garbage"]
TEXT_MALFORMED_KINDS = ["truncate", "missing_level", "bad_timestamp", "garbage"]
REQUIRED_FIELDS = ["timestamp", "level", "service"]   # removed by "missing_field"
NUMBER_FIELDS = ["status_code", "latency"]            # given text by "wrong_type"
GARBAGE_LINES = [
    "ERROR something went wrong",
    "<<<binary noise>>>",
    "{{{{",
]
TEXT_TIMESTAMP_LEN = len("2026-10-21 01:03:11,482")   # the prefix bad_timestamp replaces

# Stage 10: burst timing, as (min, max) ranges. On average, one burst a minute.
BURST_GAP_SECONDS = (30, 90)      # quiet time between bursts
BURST_LENGTH_SECONDS = (5, 15)    # how long a burst lasts
BURST_MULTIPLIER = (5, 10)        # how many times the normal rate

# If the program falls this many seconds behind schedule (for example, the
# laptop went to sleep), it skips the backlog instead of flooding to catch up.
MAX_LAG_SECONDS = 1.0


# ---------------------------------------------------------------------------
# Command-line flags
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate fake logs, one rotating file per service. "
                    "Odd-numbered services write JSON, even-numbered write plain text.")
    parser.add_argument("--rate", type=float, default=1,
                        help="total log lines per second across all services (default: 1)")
    parser.add_argument("--services", type=int, default=3,
                        help="number of services to simulate (default: 3)")
    parser.add_argument("--malformed-pct", type=float, default=0,
                        help="percentage of lines to break on purpose, 0-100 (default: 0)")
    parser.add_argument("--burst", action="store_true",
                        help="add occasional traffic bursts at 5-10x the normal rate")
    parser.add_argument("--duration", type=float, default=None,
                        help="stop after this many seconds (default: run until Ctrl+C)")
    parser.add_argument("--seed", type=int, default=None,
                        help="random seed, so runs are repeatable")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR,
                        help="folder to write the log files to (default: ./logs next to this script)")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES,
                        help=f"rotate a file when it reaches this size (default: {DEFAULT_MAX_BYTES})")
    parser.add_argument("--backup-count", type=int, default=DEFAULT_BACKUP_COUNT,
                        help=f"rotated copies to keep per service (default: {DEFAULT_BACKUP_COUNT})")
    args = parser.parse_args(argv)

    # Validate before using any value, so --rate 0 can't crash the division later.
    if args.rate <= 0:
        parser.error("--rate must be greater than 0")
    if args.services < 1:
        parser.error("--services must be at least 1")
    if not 0 <= args.malformed_pct <= 100:
        parser.error("--malformed-pct must be between 0 and 100")
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be greater than 0")
    # If either of these is 0, RotatingFileHandler silently never rotates.
    if args.max_bytes < 1:
        parser.error("--max-bytes must be at least 1")
    if args.backup_count < 1:
        parser.error("--backup-count must be at least 1")
    return args


# ---------------------------------------------------------------------------
# Building log events (Stages 1-3, 13)
# ---------------------------------------------------------------------------

def build_event(service):
    """Return one valid log event as a dictionary.

    The timestamp is a datetime; the formatters below turn it into text in
    whichever style the service uses. trace_id is None on most lines.
    """
    level = random.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
    latency = random.randint(1, 1000)
    message = random.choice(MESSAGES[level]).format(
        key=f"user:{random.randint(1, 999)}",
        latency=latency,
        n=random.randint(1, 3),
        path=random.choice(PATHS),
        user=f"u{random.randint(1000, 9999)}",
        order=random.randint(1000, 9999),
    )
    has_trace = random.random() * 100 < TRACE_ID_PCT
    return {
        "timestamp": datetime.datetime.now(datetime.timezone.utc),
        "level": level,
        "service": service,
        "host": f"{service}-host-{random.randint(1, HOSTS_PER_SERVICE)}",
        "message": message,
        "trace_id": uuid.uuid4().hex[:16] if has_trace else None,
        "status_code": random.choice(STATUS_CODES[level]),
        "latency": latency,
    }


# ---------------------------------------------------------------------------
# Rendering events as lines (Stage 12)
# ---------------------------------------------------------------------------

def format_json(event):
    """One JSON object per line, the way modern services log."""
    out = dict(event)
    out["timestamp"] = event["timestamp"].isoformat(timespec="milliseconds")
    if out["trace_id"] is None:
        del out["trace_id"]   # real JSON loggers omit absent fields rather than writing null
    return json.dumps(out)


def format_text(event):
    """Classic '<timestamp> <LEVEL> [<service>] ...' line, the way older services log."""
    ts = event["timestamp"].strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]   # keep milliseconds only
    line = (f"{ts} {event['level']} [{event['service']}] {event['host']} "
            f"{event['message']} status={event['status_code']} latency={event['latency']}ms")
    if event["trace_id"]:
        line += f" trace={event['trace_id']}"
    return line


FORMATTERS = {"json": format_json, "text": format_text}


# ---------------------------------------------------------------------------
# Breaking lines on purpose (Stage 9)
# ---------------------------------------------------------------------------

def corrupt_json(event):
    """Return a broken JSON-style line built from a valid event."""
    kind = random.choice(JSON_MALFORMED_KINDS)
    line = format_json(event)

    if kind == "truncate":
        # Cutting before the closing } means the line can never be valid JSON.
        return line[:random.randint(1, len(line) - 2)]
    if kind == "garbage":
        return random.choice(GARBAGE_LINES)

    broken = json.loads(line)   # a fresh dict, timestamp already a string
    if kind == "missing_field":
        # Still valid JSON, but a required field is gone.
        del broken[random.choice(REQUIRED_FIELDS)]
    else:
        # wrong_type: still valid JSON, but a number field holds text or null.
        broken[random.choice(NUMBER_FIELDS)] = random.choice(["fast", "N/A", None])
    return json.dumps(broken)


def corrupt_text(event):
    """Return a broken plain-text line built from a valid event."""
    kind = random.choice(TEXT_MALFORMED_KINDS)
    line = format_text(event)

    if kind == "truncate":
        return line[:random.randint(1, len(line) - 2)]
    if kind == "garbage":
        return random.choice(GARBAGE_LINES)
    if kind == "missing_level":
        # The level token is gone, so a regex expecting it will not match.
        return line.replace(f" {event['level']} ", " ", 1)
    # bad_timestamp: the timestamp prefix replaced by text no parser accepts.
    return "yesterday" + line[TEXT_TIMESTAMP_LEN:]


CORRUPTERS = {"json": corrupt_json, "text": corrupt_text}


# ---------------------------------------------------------------------------
# Writing to files (Stage 8)
# ---------------------------------------------------------------------------

def setup_logger(service, log_dir, max_bytes, backup_count):
    """Return a logger that writes bare lines to <log_dir>/<service>.log."""
    logger = logging.getLogger(service)
    logger.setLevel(logging.INFO)   # default would silently drop info() calls
    logger.propagate = False        # don't also send lines to the terminal

    if not logger.handlers:         # never add a second handler (that duplicates lines)
        handler = RotatingFileHandler(
            log_dir / f"{service}.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger


# ---------------------------------------------------------------------------
# Bursts (Stage 10)
# ---------------------------------------------------------------------------

def clock():
    """Local wall-clock time, for terminal messages you can match to Kafka UI."""
    return datetime.datetime.now().strftime("%H:%M:%S")


class BurstSchedule:
    """Decides when traffic bursts start and end.

    Between bursts the multiplier is 1. When a burst starts, the multiplier
    jumps to a random value in BURST_MULTIPLIER for a random length of time.
    """

    def __init__(self, enabled, now):
        self.enabled = enabled
        self.active = False
        self.multiplier = 1
        self.switch_at = now + random.uniform(*BURST_GAP_SECONDS)   # next start or end

    def update(self, now):
        """Start or end a burst if it's time. Return the current rate multiplier."""
        if not self.enabled or now < self.switch_at:
            return self.multiplier

        if self.active:
            self.active = False
            self.multiplier = 1
            self.switch_at = now + random.uniform(*BURST_GAP_SECONDS)
            print(f"[{clock()}] burst ended")
        else:
            self.active = True
            self.multiplier = random.randint(*BURST_MULTIPLIER)
            length = random.uniform(*BURST_LENGTH_SECONDS)
            self.switch_at = now + length
            print(f"[{clock()}] burst started: {self.multiplier}x the normal rate for {length:.0f}s")
        return self.multiplier


# ---------------------------------------------------------------------------
# Main loop (Stages 4-7, 9-12)
# ---------------------------------------------------------------------------

def run(args, loggers, formats):
    """Write lines until --duration runs out or Ctrl+C is pressed.

    Returns two dicts, keyed by service: lines written, and lines broken.
    """
    services = list(loggers)
    written = {service: 0 for service in services}
    malformed = {service: 0 for service in services}

    start = time.monotonic()
    end_at = start + args.duration if args.duration is not None else None
    next_due = start
    bursts = BurstSchedule(args.burst, start)

    try:
        while end_at is None or time.monotonic() < end_at:
            multiplier = bursts.update(time.monotonic())

            service = random.choice(services)
            fmt = formats[service]
            event = build_event(service)

            # Stage 9: break the line malformed_pct% of the time.
            # random.random() * 100 is 0 up to (not including) 100,
            # so 0 never breaks a line and 100 always does.
            if random.random() * 100 < args.malformed_pct:
                line = CORRUPTERS[fmt](event)
                malformed[service] += 1
            else:
                line = FORMATTERS[fmt](event)

            loggers[service].info(line)
            written[service] += 1

            # Stage 5: schedule the next line from when this one was due, not
            # from now, so small delays don't add up and slow the rate down.
            next_due += 1 / (args.rate * multiplier)
            now = time.monotonic()
            if now - next_due > MAX_LAG_SECONDS:
                next_due = now   # too far behind: skip the backlog

            # Don't sleep past the end of --duration.
            wake_at = next_due if end_at is None else min(next_due, end_at)
            if wake_at > now:
                time.sleep(wake_at - now)
    except KeyboardInterrupt:
        pass   # Ctrl+C is the normal way to stop; the summary still prints

    return written, malformed


def print_summary(written, malformed, formats, elapsed):
    total = sum(written.values())
    total_bad = sum(malformed.values())
    width = max(len("service"), *(len(service) for service in written))

    print()
    print(f"{'service':<{width}}  {'format':<6}  {'lines':>8}  {'malformed':>9}")
    for service in written:
        print(f"{service:<{width}}  {formats[service]:<6}  {written[service]:>8}  {malformed[service]:>9}")
    print(f"{'total':<{width}}  {'':<6}  {total:>8}  {total_bad:>9}")

    pct = 100 * total_bad / total if total else 0
    rate = total / elapsed if elapsed > 0 else 0
    print(f"\n{pct:.1f}% malformed. Ran for {elapsed:.1f}s at about {rate:.1f} lines/s.")


def main(argv=None):
    args = parse_args(argv)
    random.seed(args.seed)   # None (the default) gives a different run every time
    args.log_dir.mkdir(parents=True, exist_ok=True)

    services = [f"service-{i}" for i in range(1, args.services + 1)]
    formats = {service: FORMATS[i % len(FORMATS)] for i, service in enumerate(services)}
    loggers = {
        service: setup_logger(service, args.log_dir, args.max_bytes, args.backup_count)
        for service in services
    }

    print(f"Writing logs for {len(services)} services to {args.log_dir}")
    print("  " + ", ".join(f"{s} ({formats[s]})" for s in services))
    print(f"Rate {args.rate:g} lines/s, {args.malformed_pct:g}% malformed, "
          f"bursts {'on' if args.burst else 'off'}, trace IDs on ~{TRACE_ID_PCT}% of lines")
    if args.duration is None:
        print("Press Ctrl+C to stop.")
    else:
        print(f"Running for {args.duration:g}s (Ctrl+C stops early).")

    start = time.monotonic()
    try:
        written, malformed = run(args, loggers, formats)
    finally:
        logging.shutdown()   # flush and close every log file
    print_summary(written, malformed, formats, time.monotonic() - start)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Only reached if Ctrl+C lands before or after the main loop.
        print("\nStopped.")