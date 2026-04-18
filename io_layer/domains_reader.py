"""
Чтение входных доменов из TXT или CSV и их нормализация.
На входе — файл с одной колонкой (заголовок необязателен).
На выходе — отсортированный список уникальных доменов в нижнем регистре.
"""
import re
from pathlib import Path
from urllib.parse import urlparse
from core.logger import setup_logger

logger = setup_logger(__name__)

_SCHEME_RE = re.compile(r"^https?://", re.IGNORECASE)
_WWW_RE = re.compile(r"^www\.", re.IGNORECASE)
_TLD_RE = re.compile(r"\.[a-z]{2,}$")


def normalize_domain(raw: str) -> str | None:
    """
    https://www.SITE.com/about?x=1 → site.com
    SITE.com                       → site.com
    'not a url'                    → None
    """
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if not s:
        return None

    # Отрезаем возможные кавычки, запятые по краям (бывает в грязных CSV)
    s = s.strip(",;\"' \t")

    if not _SCHEME_RE.match(s):
        s = "http://" + s

    try:
        host = urlparse(s).hostname or ""
    except Exception as e:
        logger.debug(f"Не распарсил '{raw}': {e}")
        return None

    host = _WWW_RE.sub("", host)

    if "." not in host or not _TLD_RE.search(host):
        return None

    return host


def read_domains(path: Path) -> list[str]:
    """
    Читает файл строка-за-строкой (работает и для .txt, и для однострочного .csv).
    Игнорирует заголовки вида 'domain', 'url', 'website' и пустые строки.
    """
    if not path.exists():
        raise FileNotFoundError(path)

    raw_lines: list[str] = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Если в CSV несколько колонок — берём первую
            if "," in line:
                line = line.split(",", 1)[0].strip()
            raw_lines.append(line)

    # Срезаем заголовок, если он явно текстовый
    if raw_lines and raw_lines[0].lower() in {"domain", "url", "website", "site"}:
        raw_lines = raw_lines[1:]

    normalized = []
    for r in raw_lines:
        d = normalize_domain(r)
        if d:
            normalized.append(d)

    # Дедуп с сохранением порядка появления
    seen: set[str] = set()
    unique: list[str] = []
    for d in normalized:
        if d not in seen:
            seen.add(d)
            unique.append(d)

    logger.info(
        f"{path.name}: прочитано {len(raw_lines)} строк → "
        f"{len(normalized)} валидных доменов → {len(unique)} уникальных"
    )
    return unique
