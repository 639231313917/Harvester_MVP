"""
Единая точка настройки логирования.
Логи пишутся и в файл (с ротацией), и в stdout — удобно и локально, и на сервере.
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from config.settings import settings


def setup_logger(name: str = "leadforge") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:  # избегаем дублирования хендлеров при повторном вызове
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    try:
        settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(
            settings.LOG_DIR / "leadforge.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        pass

    logger.propagate = False
    return logger
