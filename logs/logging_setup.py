"""
LOG.info() / LOG.warning()
LOG.error()
LOG.exception()  # TraceBack
"""
from __future__ import annotations
import logging
import logging.handlers
from pathlib import Path
import sys

try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler as SafeRotatingHandler
except Exception:
    SafeRotatingHandler = logging.handlers.RotatingFileHandler  # 回退

LOGS_DIR = Path(__file__).resolve().parents[1] / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

_DEFAULT_FMT = (
    "%(asctime)s | %(levelname)s | pid=%(process)d tid=%(thread)d | "
    "%(name)s | %(filename)s:%(lineno)d | %(message)s"
)


def get_logger(
    logger_name: str,
    file_name: str | None = None,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
    level: int = logging.INFO,
    also_console: bool = True,
) -> logging.Logger:
    """
    Create/reuse a logger.
    - Multiple programs/modules will write to the same log file if they pass the same file_name.
    - Use ConcurrentRotatingFileHandler to ensure multi-process concurrent writes and rotation safety.
    """
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger  # 已创建过

    logger.setLevel(level)
    logger.propagate = False

    file_name = file_name or f"{logger_name}.log"
    log_file = LOGS_DIR / file_name

    fmt = logging.Formatter(_DEFAULT_FMT)

    fh = SafeRotatingHandler(
        filename=str(log_file),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    if also_console:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    return logger
