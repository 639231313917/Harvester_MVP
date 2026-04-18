"""Запись финального CSV с лидами. По одной строке на каждый найденный контакт."""
import pandas as pd
from pathlib import Path
from datetime import datetime
from core.models import FinalLead
from core.logger import setup_logger

logger = setup_logger(__name__)

# Порядок колонок строго по ТЗ
COLUMNS = [
    "Domain",
    "First Name",
    "Last Name",
    "Email",
    "Position",
    "Total Visits",
    "Is_adWMG_Partner",
    "Ads_Txt_Lines",
]


def write_csv(leads: list[FinalLead], output_dir: Path, prefix: str = "leadforge") -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"{prefix}_{ts}.csv"

    rows = [{
        "Domain": l.domain,
        "First Name": l.first_name or "",
        "Last Name": l.last_name or "",
        "Email": l.email,
        "Position": l.position or "",
        "Total Visits": l.total_visits if l.total_visits is not None else "",
        "Is_adWMG_Partner": "TRUE" if l.is_adwmg_partner else "FALSE",
        "Ads_Txt_Lines": l.ads_txt_lines,
    } for l in leads]

    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_csv(out_path, index=False, encoding="utf-8")

    logger.info(f"Записано {len(leads)} лидов в {out_path.name}")
    return out_path
