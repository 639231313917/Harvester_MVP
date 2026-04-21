"""
LeadForge: Scout (Agent-1) — асинхронный пайплайн сбора B2B-лидов для adWMG.

Воронка обработки:
  [1/4] Input: скачивание файла из Google Drive, нормализация и дедупликация доменов.
  [2/4] AdTech: проверка ads.txt/app-ads.txt, фильтрация (проходят те, у кого > 5 строк).
  [3/4] Traffic: проверка через SimilarWeb API (фильтрация по минимальному трафику).
  [4/4] Contacts: поиск сотрудников через Snov.io API с фильтрацией по должностям.

Выгрузка:
  - Сохранение локального CSV.
  - Загрузка CSV в целевую папку Google Drive.
  - Добавление найденных лидов в Google Таблицу (Results).
  - Запись статистики воронки в Google Таблицу (Reports).
"""

import asyncio
import sys
from pathlib import Path
from config.settings import settings
from core.logger import setup_logger
from core.models import AdsTxtInfo, FinalLead
from io_layer.domains_reader import read_domains
from io_layer.csv_writer import write_csv
from io_layer import google_io
from checkers import ads_checker
from api import similarweb, snov

# Инициализация логгера
logger = setup_logger("leadforge.main")

async def run_pipeline(input_files: list[Path]) -> Path | None:
    """Основной цикл обработки данных"""
    # Валидация настроек из .env перед стартом
    settings.validate()

    # ========== [1/4] INPUT: Чтение и нормализация ==========
    logger.info("=" * 60)
    logger.info("[1/4] INPUT: Чтение и нормализация доменов")
    logger.info("=" * 60)
    
    all_domains: list[str] = []
    seen: set[str] = set()
    
    for f in input_files:
        try:
            # Чтение доменов и дедупликация
            for d in read_domains(f):
                if d not in seen:
                    seen.add(d)
                    all_domains.append(d)
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {f.name}: {e}")

    if not all_domains:
        logger.error("Нет валидных доменов для обработки. Выход.")
        return None

    logger.info(f"[1/4] Уникальных доменов на входе: {len(all_domains)}")

    # ========== [2/4] ADTECH: Проверка ads.txt / app-ads.txt ==========
    logger.info("=" * 60)
    logger.info("[2/4] ADTECH: Проверка файлов ads.txt")
    logger.info("=" * 60)
    
    # Асинхронная проверка наличия и объема файлов
    ads_results: list[AdsTxtInfo] = await ads_checker.check_many(all_domains)
    
    # Фильтрация: оставляем тех, у кого суммарно > 5 строк
    passed_ads = ads_checker.filter_passed(ads_results)
    partners = sum(1 for r in passed_ads if r.is_adwmg_partner)

    logger.info(
        f"[2/4] Проверено: {len(ads_results)} | "
        f"Прошли фильтр (>{settings.MIN_ADS_TXT_LINES} строк): {len(passed_ads)} | "
        f"Текущие партнеры adWMG: {partners}"
    )

    if not passed_ads:
        logger.warning("Ни один домен не прошел фильтр по количеству строк в ads.txt.")
        return None

    # Создаем мапу для быстрого доступа к данным ads.txt на финальном этапе
    ads_by_domain: dict[str, AdsTxtInfo] = {r.domain: r for r in passed_ads}

    # ========== [3/4] SIMILARWEB: Проверка трафика ==========
    logger.info("=" * 60)
    logger.info("[3/4] TRAFFIC: Проверка посещаемости")
    logger.info("=" * 60)
    
    # Запрос данных SimilarWeb только для прошедших предыдущий этап
    sw_metrics = await similarweb.fetch_many(list(ads_by_domain.keys()))
    
    # Фильтрация по минимальному количеству визитов
    passed_traffic = similarweb.filter_passed(sw_metrics)

    logger.info(
        f"[3/4] Получено метрик: {len(sw_metrics)} | "
        f"Прошли порог ({settings.MIN_MONTHLY_VISITS:,} визитов): {len(passed_traffic)}"
    )

    if not passed_traffic:
        logger.warning("Нет сайтов, соответствующих требованиям по трафику.")
        return None

    # ========== [4/4] SNOV.IO: Поиск контактов ==========
    logger.info("=" * 60)
    logger.info("[4/4] CONTACTS: Поиск лиц, принимающих решения")
    logger.info(f"Используется ключевых слов для поиска должностей: {len(settings.POSITION_KEYWORDS)}")
    logger.info("=" * 60)
    
    # Поиск сотрудников через Domain Search API с фильтром по ключевым словам
    contacts_by_domain = await snov.find_contacts_many(passed_traffic)

    logger.info(
        f"[4/4] Доменов с подходящими контактами: {len(contacts_by_domain)}"
    )

    if not contacts_by_domain:
        logger.warning("Подходящие контакты не найдены.")
        return None

    # ========== ФИНАЛЬНАЯ СБОРКА И ЭКСПОРТ ==========
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

    # 1. Сохранение локального CSV файла
    out_local = write_csv(final_leads, settings.OUTPUT_DIR)

    # 2. Интеграция с Google Drive и Sheets
    try:
        # Загрузка CSV в облачную папку
        google_io.upload_csv_to_drive(out_local)
        
        # Добавление лидов в основную таблицу результатов
        google_io.append_leads_to_sheet(final_leads)
        
        # Запись статистики воронки в таблицу отчетов
        stats = {
            'total_input': len(all_domains),
            'passed_ads': len(passed_ads),
            'passed_traffic': len(passed_traffic),
            'domains_with_contacts': len(contacts_by_domain),
            'total_contacts_found': len(final_leads)
        }
        google_io.append_report_to_sheet(stats)
    except Exception as e:
        logger.error(f"Ошибка при работе с Google API: {e}")

    # Итоговый вывод в консоль
    logger.info("=" * 60)
    logger.info("ВОРОНКА ЗАВЕРШЕНА:")
    logger.info(f"  1. Вход:              {len(all_domains)}")
    logger.info(f"  2. ads.txt > {settings.MIN_ADS_TXT_LINES}:      {len(passed_ads)}")
    logger.info(f"  3. Трафик OK:         {len(passed_traffic)}")
    logger.info(f"  4. Лиды найдены:      {len(final_leads)} контактов")
    logger.info(f"Локальный файл: {out_local.name}")
    logger.info("=" * 60)

    return out_local

def main():
    """Точка входа в программу"""
    # Шаг 0: Скачивание актуального списка доменов из Google Drive
    try:
        input_file = google_io.download_input_file(settings.INPUT_DIR)
    except Exception as e:
        logger.error(f"Не удалось получить входной файл из Google Drive: {e}")
        sys.exit(1)

    # Запуск асинхронного пайплайна
    asyncio.run(run_pipeline([input_file]))

if __name__ == "__main__":
    main()