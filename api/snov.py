"""
Шаг 4 воронки: Snov.io Domain Search API.

Auth: OAuth client_credentials (User ID + Secret из кабинета Snov).
Endpoint: /v2/domain-emails-with-info (новая версия с именем/фамилией/позицией).

Для каждого домена тянем до N сотрудников, фильтруем по ключевым словам
в position (подстрока без учёта регистра). Возвращаем все подходящие контакты.
"""
import asyncio
import aiohttp
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential
from core.models import Contact
from core.logger import setup_logger
from config.settings import settings

logger = setup_logger(__name__)

_TOKEN_URL = "https://api.snov.io/v1/oauth/access_token"
_DOMAIN_SEARCH_URL = "https://api.snov.io/v2/domain-emails-with-info"

# Сколько сотрудников запрашивать на один домен за раз (API поддерживает пагинацию)
_PER_DOMAIN_LIMIT = 100


def _position_matches(position: str | None, keywords: list[str]) -> bool:
    """Подстрока без учёта регистра. Пустая позиция — не матч."""
    if not position:
        return False
    p = position.lower()
    return any(kw in p for kw in keywords)


class SnovClient:
    def __init__(self):
        self._token: str | None = None
        self._token_lock = asyncio.Lock()

    async def _get_token(self, session: aiohttp.ClientSession) -> str:
        async with self._token_lock:
            if self._token:
                return self._token

            payload = {
                "grant_type": "client_credentials",
                "client_id": settings.SNOV_CLIENT_ID,
                "client_secret": settings.SNOV_CLIENT_SECRET,
            }
            async with session.post(
                _TOKEN_URL, data=payload, timeout=ClientTimeout(total=20)
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                self._token = data["access_token"]
                logger.info("Snov.io: токен получен")
                return self._token

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    async def search_domain(
        self,
        session: aiohttp.ClientSession,
        domain: str,
    ) -> list[Contact]:
        """
        Дёргает /v2/domain-emails-with-info и возвращает ВСЕХ сотрудников
        (без фильтра — фильтр применяется отдельно).
        """
        token = await self._get_token(session)
        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "domain": domain,
            "type": "all",
            "limit": _PER_DOMAIN_LIMIT,
            "lastId": 0,
        }
        timeout = ClientTimeout(total=settings.REQUEST_TIMEOUT + 10)

        try:
            async with session.get(
                _DOMAIN_SEARCH_URL, headers=headers, params=params, timeout=timeout
            ) as resp:
                if resp.status == 401:
                    # токен протух — сбрасываем, tenacity повторит запрос
                    self._token = None
                    raise RuntimeError(f"Snov.io 401 на домене {domain}")
                if resp.status == 402:
                    logger.error(f"Snov.io 402 (нет кредитов) на {domain}")
                    return []
                if resp.status != 200:
                    logger.warning(f"Snov.io {domain}: HTTP {resp.status}")
                    return []

                data = await resp.json()
        except asyncio.TimeoutError:
            logger.warning(f"Snov.io {domain}: timeout")
            return []

        # Структура ответа v2: {"success": true, "data": {"emails": [...]}}
        emails_block = (data.get("data") or {}).get("emails") or data.get("emails") or []
        if not isinstance(emails_block, list):
            return []

        contacts: list[Contact] = []
        for item in emails_block:
            email = item.get("email")
            if not email:
                continue
            contacts.append(Contact(
                first_name=item.get("firstName") or item.get("first_name"),
                last_name=item.get("lastName") or item.get("last_name"),
                email=email,
                position=item.get("position"),
            ))
        return contacts


async def find_contacts_many(domains: list[str]) -> dict[str, list[Contact]]:
    """
    Для каждого домена получаем список сотрудников и фильтруем по ключевым словам.
    Возвращаем словарь: {domain: [Contact, ...]} — только подходящие.
    Домены без подходящих сотрудников в результат не попадают.
    """
    keywords = settings.POSITION_KEYWORDS
    client = SnovClient()
    sem = asyncio.Semaphore(settings.API_CONCURRENCY_LIMIT)
    results: dict[str, list[Contact]] = {}

    async with aiohttp.ClientSession() as session:
        async def _one(d: str):
            async with sem:
                try:
                    all_contacts = await client.search_domain(session, d)
                except Exception as e:
                    logger.warning(f"Snov.io {d} упал: {e}")
                    return d, []
                matched = [c for c in all_contacts if _position_matches(c.position, keywords)]
                return d, matched

        tasks = [_one(d) for d in domains]
        for coro in asyncio.as_completed(tasks):
            d, matched = await coro
            if matched:
                results[d] = matched

    total_contacts = sum(len(v) for v in results.values())
    logger.info(
        f"Snov.io: обработано {len(domains)} доменов → "
        f"с контактами {len(results)}, всего подходящих контактов: {total_contacts}"
    )
    return results
