# logging_setup.py
import logging
import os
import sys

def _resolve_level(name: str) -> int:
    try:
        return getattr(logging, str(name).upper())
    except Exception:
        return logging.INFO

def _build_logger() -> logging.Logger:
    level = _resolve_level(os.getenv("LOG_LEVEL", "INFO"))
    log_file = os.getenv("LOG_FILE", "C:/slurs/slursbot.log")

    lg = logging.getLogger("slursbot")
    lg.setLevel(level)
    lg.propagate = False

    # Prevent duplicate handlers if setup_logger is called more than once
    if not lg.handlers:
        fmt = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(fmt)
        lg.addHandler(sh)

        # File (best-effort)
        try:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setLevel(level)
            fh.setFormatter(fmt)
            lg.addHandler(fh)
        except Exception:
            lg.warning("Could not open log file at %s; console only.", log_file)

    return lg

# Public API
def setup_logger() -> logging.Logger:
    return _build_logger()

# Keep this for back-compat (modules may call getLogger("slursbot"))
logger = logging.getLogger("slursbot")

def set_level(name: str) -> None:
    new_level = _resolve_level(name)
    lg = logging.getLogger("slursbot")
    lg.setLevel(new_level)
    for h in lg.handlers:
        h.setLevel(new_level)

__all__ = ["logger", "set_level", "setup_logger"]
