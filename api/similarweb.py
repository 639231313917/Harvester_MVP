"""
Шаг 3 воронки: SimilarWeb через RapidAPI.

Цель: получить total_visits для домена.
Фильтр: total_visits >= MIN_MONTHLY_VISITS.
"""
import asyncio
import aiohttp
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from core.models import SWMetrics
from core.logger import setup_logger
from config.settings import settings

logger = setup_logger(__name__)


class QuotaExceededError(Exception):
    """429 от rapidapi — дальше слать запросы бессмысленно."""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    reraise=True,
)
async def fetch_visits(session: aiohttp.ClientSession, domain: str) -> SWMetrics:
    """Получаем только total_visits — больше ничего из SimilarWeb не нужно."""
    url = f"https://{settings.SIMILARWEB_HOST}/DomainInfo"
    headers = {
        "X-RapidAPI-Key": settings.SIMILARWEB_API_KEY,
        "X-RapidAPI-Host": settings.SIMILARWEB_HOST,
    }
    params = {"domain": domain}
    timeout = ClientTimeout(total=settings.REQUEST_TIMEOUT)

    async with session.get(url, headers=headers, params=params, timeout=timeout) as resp:
        if resp.status == 429:
            raise QuotaExceededError(f"SimilarWeb 429 на домене {domain}")
        if resp.status != 200:
            logger.warning(f"SimilarWeb {domain}: HTTP {resp.status}")
            return SWMetrics(domain=domain, total_visits=None)

        try:
            data = await resp.json()
        except Exception as e:
            logger.warning(f"SimilarWeb {domain}: битый JSON ({e})")
            return SWMetrics(domain=domain, total_visits=None)

    # Ключ зависит от версии API — пытаемся и с опечаткой (Engagments), и без
    engagements = data.get("Engagments") or data.get("Engagements") or {}
    visits_raw = engagements.get("Visits") if isinstance(engagements, dict) else None

    try:
        visits = int(float(visits_raw)) if visits_raw is not None else None
    except (TypeError, ValueError):
        visits = None

    return SWMetrics(domain=domain, total_visits=visits)


async def fetch_many(domains: list[str]) -> dict[str, SWMetrics]:
    """
    Батч-запрос с ограниченным параллелизмом (квота платная — не газуем).
    При 429 прерываем батч и сохраняем уже полученное.
    """
    sem = asyncio.Semaphore(settings.API_CONCURRENCY_LIMIT)
    results: dict[str, SWMetrics] = {}

    async with aiohttp.ClientSession() as session:
        async def _one(d: str):
            async with sem:
                try:
                    return await fetch_visits(session, d)
                except QuotaExceededError:
                    raise
                except Exception as e:
                    logger.warning(f"SimilarWeb {d} упал: {e}")
                    return SWMetrics(domain=d, total_visits=None)

        tasks = [asyncio.create_task(_one(d)) for d in domains]
        try:
            for t in asyncio.as_completed(tasks):
                m = await t
                results[m.domain] = m
        except QuotaExceededError:
            logger.error("Квота SimilarWeb исчерпана, прерываю батч. Сохраняю собранное.")
            for t in tasks:
                if not t.done():
                    t.cancel()

    logger.info(f"SimilarWeb: получено метрик {len(results)}/{len(domains)}")
    return results


def filter_passed(metrics: dict[str, SWMetrics]) -> list[str]:
    """Домены, прошедшие порог трафика."""
    threshold = settings.MIN_MONTHLY_VISITS
    passed = [
        m.domain for m in metrics.values()
        if m.total_visits is not None and m.total_visits >= threshold
    ]
    return passed
