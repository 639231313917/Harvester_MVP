import io
import gspread
from pathlib import Path
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

from config.settings import settings
from core.logger import setup_logger
from core.models import FinalLead

logger = setup_logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_credentials():
    return Credentials.from_service_account_file(settings.GOOGLE_CREDS_PATH, scopes=SCOPES)

def download_input_file(output_dir: Path) -> Path:
    """Скачивает файл с доменами из Google Drive в локальную папку data/input/"""
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)
    
    file_id = settings.DRIVE_INPUT_FILE_ID
    logger.info(f"Скачиваю входной файл из Google Drive (ID: {file_id})...")
    
    request = service.files().get_media(fileId=file_id)
    file_path = output_dir / "google_input_domains.csv"
    
    with io.FileIO(file_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            
    logger.info("Входной файл успешно скачан.")
    return file_path

def upload_csv_to_drive(file_path: Path):
    """Загружает сгенерированный CSV-файл в целевую папку на Google Drive"""
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)
    
    file_metadata = {
        'name': file_path.name,
        'parents': [settings.DRIVE_OUTPUT_FOLDER_ID]
    }
    media = MediaFileUpload(str(file_path), mimetype='text/csv')
    
    logger.info(f"Загружаю {file_path.name} в Google Drive...")
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    logger.info(f"Файл загружен. ID: {file.get('id')}")

def append_leads_to_sheet(leads: list[FinalLead]):
    """Добавляет найденные контакты в итоговую Google Таблицу (нижними строками)"""
    if not leads:
        return
        
    client = gspread.authorize(get_credentials())
    sheet = client.open_by_key(settings.SHEET_RESULTS_ID).sheet1
    
    rows = []
    for l in leads:
        rows.append([
            l.domain,
            l.first_name or "",
            l.last_name or "",
            l.email,
            l.position or "",
            l.total_visits if l.total_visits is not None else "",
            "TRUE" if l.is_adwmg_partner else "FALSE",
            l.ads_txt_lines
        ])
    
    logger.info(f"Экспортирую {len(rows)} лидов в Google Sheets...")
    sheet.append_rows(rows, value_input_option='USER_ENTERED')
    logger.info("Экспорт в Google Sheets завершен.")

def append_report_to_sheet(stats: dict):
    """Добавляет одну строку с отчетом о воронке в таблицу отчетов"""
    client = gspread.authorize(get_credentials())
    sheet = client.open_by_key(settings.SHEET_REPORTS_ID).sheet1
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    row = [
        date_str,
        stats.get('total_input', 0),
        stats.get('passed_ads', 0),
        stats.get('passed_traffic', 0),
        stats.get('domains_with_contacts', 0),
        stats.get('total_contacts_found', 0)
    ]
    
    sheet.append_row(row, value_input_option='USER_ENTERED')
    logger.info("Отчет о воронке сохранен в Google Sheets.")