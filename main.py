"""
LeadForge: Scout (Agent-1) — асинхронный пайплайн сбора B2B-лидов для adWMG.

Воронка:
  [1/4] Input: читаем домены из data/input/, нормализуем, дедуплицируем
  [2/4] AdTech: проверяем ads.txt/app-ads.txt, отсеиваем тех у кого ≤ порога строк
  [3/4] SimilarWeb: проверяем трафик, отсеиваем ниже порога
  [4/4] Snov.io: ищем контакты, фильтруем по ключевым словам в позиции

Output: CSV в data/output/ — по строке на каждый найденный подходящий контакт.
"""
import asyncio
import sys
from pathlib import Path
from config.settings import settings
from core.logger import setup_logger
from core.models import AdsTxtInfo, FinalLead
from io_layer.domains_reader import read_domains
from io_layer.csv_writer import write_csv
from checkers import ads_checker
from api import similarweb, snov

logger = setup_logger("leadforge.main")


async def run_pipeline(input_files: list[Path]) -> Path | None:
    settings.validate()

    # ========== [1/4] INPUT ==========
    logger.info("=" * 60)
    logger.info("[1/4] INPUT: чтение и нормализация доменов")
    logger.info("=" * 60)
    all_domains: list[str] = []
    seen: set[str] = set()
    for f in input_files:
        try:
            for d in read_domains(f):
                if d not in seen:
                    seen.add(d)
                    all_domains.append(d)
        except Exception as e:
            logger.error(f"Не смог прочитать {f}: {e}")

    if not all_domains:
        logger.error("Нет валидных доменов во входных файлах — выхожу.")
        return None

    logger.info(f"[1/4] Всего уникальных доменов на входе: {len(all_domains)}")

    # ========== [2/4] ADS.TXT ==========
    logger.info("=" * 60)
    logger.info("[2/4] ADTECH: проверка ads.txt / app-ads.txt")
    logger.info("=" * 60)
    ads_results: list[AdsTxtInfo] = await ads_checker.check_many(all_domains)

    with_ads_any = sum(1 for r in ads_results if r.has_ads_txt or r.has_app_ads_txt)
    passed_ads = ads_checker.filter_passed(ads_results)
    partners = sum(1 for r in passed_ads if r.is_adwmg_partner)

    logger.info(
        f"[2/4] Проверено: {len(ads_results)} | "
        f"с хоть каким-то ads.txt: {with_ads_any} | "
        f"прошли фильтр (>{settings.MIN_ADS_TXT_LINES} строк): {len(passed_ads)} | "
        f"из них партнёры adWMG: {partners}"
    )

    if not passed_ads:
        logger.warning("Ни один домен не прошёл фильтр ads.txt — выхожу.")
        return None

    # Кладём инфо о ads.txt в словарь — понадобится на финальной сборке
    ads_by_domain: dict[str, AdsTxtInfo] = {r.domain: r for r in passed_ads}

    # ========== [3/4] SIMILARWEB ==========
    logger.info("=" * 60)
    logger.info("[3/4] TRAFFIC: проверка SimilarWeb")
    logger.info("=" * 60)
    sw_metrics = await similarweb.fetch_many(list(ads_by_domain.keys()))
    passed_traffic = similarweb.filter_passed(sw_metrics)

    logger.info(
        f"[3/4] Отправлено запросов: {len(ads_by_domain)} | "
        f"получено метрик: {len(sw_metrics)} | "
        f"прошли фильтр (≥{settings.MIN_MONTHLY_VISITS:,} визитов): {len(passed_traffic)}"
    )

    if not passed_traffic:
        logger.warning("Ни один домен не прошёл фильтр SimilarWeb — выхожу.")
        return None

    # ========== [4/4] SNOV.IO ==========
    logger.info("=" * 60)
    logger.info("[4/4] CONTACTS: поиск контактов через Snov.io")
    logger.info(f"    Ключевых слов для фильтра позиций: {len(settings.POSITION_KEYWORDS)}")
    logger.info("=" * 60)
    contacts_by_domain = await snov.find_contacts_many(passed_traffic)

    logger.info(
        f"[4/4] Отправлено запросов: {len(passed_traffic)} | "
        f"доменов с подходящими контактами: {len(contacts_by_domain)}"
    )

    if not contacts_by_domain:
        logger.warning("Ни на одном домене не нашлось подходящих контактов — выхожу.")
        return None

    # ========== СБОРКА ФИНАЛЬНЫХ ЛИДОВ ==========
    final_leads: list[FinalLead] = []
    for domain, contacts in contacts_by_domain.items():
        ads_info = ads_by_domain.get(domain)
        sw = sw_metrics.get(domain)

        for c in contacts:
            final_leads.append(FinalLead(
                domain=domain,
                first_name=c.first_name,
                last_name=c.last_name,
                email=c.email,
                position=c.position,
                total_visits=sw.total_visits if sw else None,
                is_adwmg_partner=ads_info.is_adwmg_partner if ads_info else False,
                ads_txt_lines=ads_info.total_lines if ads_info else 0,
            ))

    out = write_csv(final_leads, settings.OUTPUT_DIR)

    # ========== ИТОГОВАЯ ВОРОНКА ==========
    logger.info("=" * 60)
    logger.info("ВОРОНКА:")
    logger.info(f"  1. Вход:            {len(all_domains)}")
    logger.info(f"  2. ads.txt OK:      {len(passed_ads)}  ({len(passed_ads)*100//len(all_domains)}%)")
    logger.info(f"  3. Трафик OK:       {len(passed_traffic)}  ({len(passed_traffic)*100//len(all_domains)}%)")
    logger.info(f"  4. Контакты найдены: {len(contacts_by_domain)} доменов → {len(final_leads)} строк")
    logger.info(f"Файл: {out}")
    logger.info("=" * 60)

    return out


def main():
    # Принимаем и .txt, и .csv (в CSV читаем только первую колонку)
    input_files = sorted(
        list(settings.INPUT_DIR.glob("*.txt")) + list(settings.INPUT_DIR.glob("*.csv"))
    )
    if not input_files:
        logger.error(
            f"Не найдено .txt или .csv в {settings.INPUT_DIR}. "
            f"Положите файл с голыми доменами (один домен на строку)."
        )
        sys.exit(1)

    asyncio.run(run_pipeline(input_files))


if __name__ == "__main__":
    main()
