# LeadForge: Scout (Agent-1)

Асинхронный пайплайн сбора B2B-лидов для AdTech-компании **adWMG**.

На вход — голые домены, на выход — таблица с контактами AdOps/Programmatic/Monetization-специалистов из сайтов, которые реально монетизируются и имеют трафик.

## Воронка

```
[1/4] Input          — читаем TXT/CSV из data/input/, нормализуем, дедуплицируем
[2/4] ads.txt        — проверяем /ads.txt и /app-ads.txt, считаем валидные строки
                       → проходят только те, у кого в сумме > 5 строк
                       → маркер adwmg.com → флаг Is_adWMG_Partner
[3/4] SimilarWeb     — получаем total_visits
                       → проходят только те, у кого >= MIN_MONTHLY_VISITS
[4/4] Snov.io        — /v2/domain-emails-with-info, все сотрудники
                       → фильтр по keywords.txt (подстрока, без учёта регистра)
```

## Быстрый старт

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# впиши ключи SIMILARWEB_API_KEY, SNOV_CLIENT_ID, SNOV_CLIENT_SECRET

# положи файл с голыми доменами в data/input/
# один домен на строку, без протоколов и www обязательно — скрипт сам почистит

python main.py
```

Итог — `data/output/leadforge_YYYYMMDD_HHMMSS.csv`.

## Формат входных файлов

Один домен на строку. Принимается и `.txt`, и `.csv` (из CSV берётся первая колонка).

```
nytimes.com
https://www.forbes.com
techcrunch.com
bbc.com/about
```

Скрипт приведёт всё к чистому виду: `nytimes.com`, `forbes.com`, `techcrunch.com`, `bbc.com`. Дубли уберутся.

## Формат выходного CSV

| Domain | First Name | Last Name | Email | Position | Total Visits | Is_adWMG_Partner | Ads_Txt_Lines |
|--------|-----------|-----------|-------|----------|--------------|------------------|---------------|
| nytimes.com | John | Smith | j.smith@nytimes.com | Head of Programmatic | 550000000 | FALSE | 342 |
| nytimes.com | Jane | Doe | j.doe@nytimes.com | Ad Ops Manager | 550000000 | FALSE | 342 |

Один домен → столько строк, сколько нашлось подходящих контактов.

## Настройка ключевых слов

Файл `config/keywords.txt`. По одному ключевому слову на строку. Матчинг — подстрока без учёта регистра, так что `programmatic` поймает `Senior Programmatic Manager`, `Head of Programmatic Partnerships` и т.п.

```
programmatic
monetization
ad ops
yield
```

Можно указывать словосочетания — пробелы сохраняются.

## Пороги и производительность (`.env`)

| Переменная | Что делает | Дефолт |
|-----------|-----------|--------|
| `MIN_MONTHLY_VISITS` | минимум визитов для прохождения Шага 3 | 100000 |
| `MIN_ADS_TXT_LINES` | минимум строк в ads.txt+app-ads.txt (строго больше) | 5 |
| `CONCURRENCY_LIMIT` | параллелизм для ads.txt (сайты держат хорошо) | 30 |
| `API_CONCURRENCY_LIMIT` | параллелизм для SimilarWeb и Snov.io | 10 |
| `REQUEST_TIMEOUT` | таймаут на один HTTP-запрос, сек | 15 |

## Структура проекта

```
leadforge-scout/
├── main.py                     # Оркестратор воронки
├── config/
│   ├── settings.py             # .env + пороги
│   └── keywords.txt            # ключевые слова для фильтра позиций
├── core/
│   ├── logger.py               # логирование с ротацией
│   └── models.py               # Pydantic: AdsTxtInfo, SWMetrics, Contact, FinalLead
├── io_layer/
│   ├── domains_reader.py       # чтение TXT/CSV + нормализация + дедуп
│   └── csv_writer.py           # финальный CSV
├── checkers/
│   └── ads_checker.py          # Шаг 2: ads.txt / app-ads.txt
├── api/
│   ├── similarweb.py           # Шаг 3: трафик
│   └── snov.py                 # Шаг 4: контакты
├── data/
│   ├── input/                  # сюда кладутся списки доменов
│   └── output/                 # отсюда забираются результаты
└── logs/
    └── leadforge.log           # с ротацией 5 МБ × 3
```

## Деплой как Background Worker

Скрипт запускается одной командой `python main.py` и нормально уходит в фон. На Render, Railway или любом VPS просто укажи это стартовой командой и прокинь секреты через переменные окружения.

## Что логируется

На каждом этапе видно размер воронки:

```
[1/4] INPUT: чтение и нормализация доменов
[1/4] Всего уникальных доменов на входе: 10000
[2/4] Проверено: 10000 | с хоть каким-то ads.txt: 3847 | прошли фильтр (>5): 2134 | из них партнёры adWMG: 87
[3/4] Отправлено запросов: 2134 | получено метрик: 2098 | прошли фильтр (≥100,000): 612
[4/4] Отправлено запросов: 612 | доменов с подходящими контактами: 289
ВОРОНКА:
  1. Вход:            10000
  2. ads.txt OK:      2134  (21%)
  3. Трафик OK:       612  (6%)
  4. Контакты найдены: 289 доменов → 847 строк
```
