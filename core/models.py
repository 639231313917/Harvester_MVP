"""
Типизированные модели этапов воронки.
Pydantic даёт валидацию и понятную форму данных между шагами.
"""
from typing import Optional
from pydantic import BaseModel


class AdsTxtInfo(BaseModel):
    """Результат Шага 2: проверка ads.txt / app-ads.txt."""
    domain: str
    has_ads_txt: bool = False
    has_app_ads_txt: bool = False
    ads_txt_lines: int = 0       # валидные строки в ads.txt (без # и пустых)
    app_ads_txt_lines: int = 0   # то же для app-ads.txt
    is_adwmg_partner: bool = False

    @property
    def total_lines(self) -> int:
        return self.ads_txt_lines + self.app_ads_txt_lines


class SWMetrics(BaseModel):
    """Результат Шага 3: трафик SimilarWeb."""
    domain: str
    total_visits: Optional[int] = None


class Contact(BaseModel):
    """Один сотрудник, найденный в Snov.io."""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: str
    position: Optional[str] = None


class FinalLead(BaseModel):
    """
    Финальная строка CSV. Один домен может породить несколько FinalLead —
    по одному на каждый найденный подходящий контакт.
    """
    domain: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: str
    position: Optional[str] = None
    total_visits: Optional[int] = None
    is_adwmg_partner: bool = False
    ads_txt_lines: int = 0  # суммарное количество строк (ads.txt + app-ads.txt)
