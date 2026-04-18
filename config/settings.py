"""
Централизованная конфигурация LeadForge: Scout.
Все параметры тянутся из .env, с разумными дефолтами.
Ключевые слова для фильтра позиций — из config/keywords.txt.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


def _load_keywords(path: Path) -> list[str]:
    """Читает ключевые слова построчно, игнорируя пустые и комментарии."""
    if not path.exists():
        return []
    out: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            out.append(s.lower())
    return out


class Settings:
    # --- API credentials ---
    SIMILARWEB_API_KEY: str = os.getenv("SIMILARWEB_API_KEY", "")
    SIMILARWEB_HOST: str = os.getenv("SIMILARWEB_HOST", "similarweb12.p.rapidapi.com")

    SNOV_CLIENT_ID: str = os.getenv("SNOV_CLIENT_ID", "")
    SNOV_CLIENT_SECRET: str = os.getenv("SNOV_CLIENT_SECRET", "")

    # --- Фильтрация ---
    MIN_MONTHLY_VISITS: int = int(os.getenv("MIN_MONTHLY_VISITS", "100000"))
    MIN_ADS_TXT_LINES: int = int(os.getenv("MIN_ADS_TXT_LINES", "5"))
    ADWMG_MARKER: str = "adwmg.com"

    # --- Ключевые слова для фильтра позиций ---
    POSITION_KEYWORDS: list[str] = _load_keywords(BASE_DIR / "config" / "keywords.txt")

    # --- Пути ---
    INPUT_DIR: Path = BASE_DIR / "data" / "input"
    OUTPUT_DIR: Path = BASE_DIR / "data" / "output"
    LOG_DIR: Path = BASE_DIR / "logs"

    # --- Производительность ---
    CONCURRENCY_LIMIT: int = int(os.getenv("CONCURRENCY_LIMIT", "30"))
    API_CONCURRENCY_LIMIT: int = int(os.getenv("API_CONCURRENCY_LIMIT", "10"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "15"))

    USER_AGENT: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    def validate(self) -> None:
        """Критичная проверка перед стартом — чтобы не упасть на середине."""
        missing = []
        if not self.SIMILARWEB_API_KEY:
            missing.append("SIMILARWEB_API_KEY")
        if not (self.SNOV_CLIENT_ID and self.SNOV_CLIENT_SECRET):
            missing.append("SNOV_CLIENT_ID/SECRET")
        if missing:
            raise RuntimeError(
                f"Отсутствуют переменные окружения: {', '.join(missing)}. "
                f"Проверь .env"
            )

        if not self.POSITION_KEYWORDS:
            raise RuntimeError(
                "Не задано ни одного ключевого слова в config/keywords.txt. "
                "Без них Snov-фильтр не знает, кого искать."
            )

        for d in (self.INPUT_DIR, self.OUTPUT_DIR, self.LOG_DIR):
            d.mkdir(parents=True, exist_ok=True)


settings = Settings()
