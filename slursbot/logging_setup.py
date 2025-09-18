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

    # Avoid duplicate handlers if this module is imported more than once
    if not lg.handlers:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"
        )

        # Console
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(formatter)
        lg.addHandler(sh)

        # File (best-effort)
        try:
            if log_file:
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                fh = logging.FileHandler(log_file, encoding="utf-8")
                fh.setLevel(level)
                fh.setFormatter(formatter)
                lg.addHandler(fh)
        except Exception:
            # If we can't write the file, still keep console logging
            pass

    return lg

# ---- public API -------------------------------------------------------------

logger: logging.Logger = _build_logger()

def set_level(name: str) -> None:
    """Dynamically change logger level for both logger and its handlers."""
    new_level = _resolve_level(name)
    logger.setLevel(new_level)
    for h in logger.handlers:
        h.setLevel(new_level)

__all__ = ["logger", "set_level"]
