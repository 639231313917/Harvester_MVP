"""
Шаг 2 воронки: проверка ads.txt и app-ads.txt.

Логика:
  - Дёргаем GET https://{domain}/ads.txt и https://{domain}/app-ads.txt
  - Если файл есть (HTTP 200, не HTML) — считаем валидные строки
    (игнорируем пустые и комментарии '#')
  - Ставим флаг is_adwmg_partner, если в текстах встречается 'adwmg.com'
  - Домен проходит дальше, только если суммарно валидных строк > MIN_ADS_TXT_LINES
"""
import asyncio
import aiohttp
from aiohttp import ClientTimeout
from core.models import AdsTxtInfo
from core.logger import setup_logger
from config.settings import settings

logger = setup_logger(__name__)


def _count_valid_lines(text: str) -> int:
    """Строки, не пустые и не начинающиеся с '#'."""
    count = 0
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        count += 1
    return count


async def _fetch_text(session: aiohttp.ClientSession, url: str, domain: str) -> str | None:
    """
    Загружает содержимое ads.txt/app-ads.txt.
    Возвращает None, если файла нет или это HTML-заглушка (кастомный 404).
    """
    timeout = ClientTimeout(total=settings.REQUEST_TIMEOUT)
    try:
        async with session.get(url, timeout=timeout, allow_redirects=True, ssl=False) as resp:
            if resp.status != 200:
                return None

            # Защита от сайтов, отдающих HTML вместо ошибки 404
            ctype = resp.headers.get("Content-Type", "").lower()
            if "text/html" in ctype:
                return None

            # Защита от редиректа на чужой домен
            final_host = (resp.url.host or "").lstrip("www.") if resp.url else ""
            clean_domain = domain.lstrip("www.")
            if clean_domain not in final_host:
                return None

            return await resp.text(errors="replace")
    except (asyncio.TimeoutError, aiohttp.ClientError):
        return None
    except Exception as e:
        logger.debug(f"ads.txt {domain}: {type(e).__name__}: {e}")
        return None


async def check_domain(session: aiohttp.ClientSession, domain: str) -> AdsTxtInfo:
    """Полная проверка одного домена: ads.txt + app-ads.txt + поиск adwmg."""
    ads_url = f"https://{domain}/ads.txt"
    app_url = f"https://{domain}/app-ads.txt"

    ads_text, app_text = await asyncio.gather(
        _fetch_text(session, ads_url, domain),
        _fetch_text(session, app_url, domain),
    )

    ads_lines = _count_valid_lines(ads_text) if ads_text else 0
    app_lines = _count_valid_lines(app_text) if app_text else 0

    combined = ((ads_text or "") + "\n" + (app_text or "")).lower()
    partner = settings.ADWMG_MARKER.lower() in combined

    return AdsTxtInfo(
        domain=domain,
        has_ads_txt=bool(ads_text),
        has_app_ads_txt=bool(app_text),
        ads_txt_lines=ads_lines,
        app_ads_txt_lines=app_lines,
        is_adwmg_partner=partner,
    )


async def check_many(domains: list[str]) -> list[AdsTxtInfo]:
    """Батч-проверка. Возвращает AdsTxtInfo для каждого домена (даже для упавших)."""
    sem = asyncio.Semaphore(settings.CONCURRENCY_LIMIT)
    headers = {"User-Agent": settings.USER_AGENT}
    connector = aiohttp.TCPConnector(limit=settings.CONCURRENCY_LIMIT + 20, ssl=False)

    async def _one(session, d):
        async with sem:
            return await check_domain(session, d)

    results: list[AdsTxtInfo] = []
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [_one(session, d) for d in domains]
        for coro in asyncio.as_completed(tasks):
            info = await coro
            results.append(info)

    return results


def filter_passed(infos: list[AdsTxtInfo]) -> list[AdsTxtInfo]:
    """Отбираем только те домены, у которых суммарно строк > порога."""
    threshold = settings.MIN_ADS_TXT_LINES
    return [i for i in infos if i.total_lines > threshold]
