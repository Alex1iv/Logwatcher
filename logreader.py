from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
import json
import os
import re


def is_recent_event(event, max_age_days: int):
    """Проверка является ли строка лога недавней по времени
    """    
    cutoff = datetime.now() - timedelta(days=max_age_days)
    return event["timestamp"] >= cutoff


def load_state(STATE_FILE, logger=None):
    """Загружаем точки остановки чтения файлов
    """    
    if not Path(STATE_FILE).exists():
        if logger:
            logger.error("[STATE] Предыдущие точки остановки чтения логов не загружены")
        return {}
    
    with open(STATE_FILE, "r") as f:
        raw_state = json.load(f)
    return {str(k): v for k, v in raw_state.items()}

def save_state(STATE_FILE, state:dict, logger=None, file_path=None, offset=None):
    """Сохраняем точки остановки чтения файлов

    Args:
        state (dict): _description_
        
    """    
    Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
    
    state = dict(sorted(state.items())) # сортировка
    
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    if logger and file_path is not None and offset is not None:
        # Сохранены актуальные точки остановки чтения логов файл 
        logger.info(
            f"[STATE] Сохранены актуальные точки остановки чтения логов {file_path} {offset}.")


def discover_files(LOG_DIR, FILE_PATTERN, logger=None):
    """Читаем лог-файлы, названия которых удовлетворяет шаблону в config.yaml
    Discover log files using regex from config.
    """    
    try:
        regex = re.compile(FILE_PATTERN)
    except re.error as e:
        raise ValueError(f"Invalid file_name_regex: {FILE_PATTERN}") from e

    matched_files = []
    
    # поиск по названиям файлов
    for name in os.listdir(LOG_DIR):
        if regex.match(name): # Название удовлетворяет условию
            full_path = Path(LOG_DIR, name)
            matched_files.append(full_path)
            if logger:
                logger.info(f"[read] Matched log files: {full_path}") #name
    if logger:
        logger.info(f"[read] Total matched log files: {len(matched_files)}")

    return matched_files

def read_new_lines_with_progress(file_path: str, start_offset:int, show_progress:bool=True):
    """
    Reads file line by line.
    Returns (new_offset, list_of_lines)
    show_progress (bool) - displays (if True) or hides (if False) the progress bar
    """
    
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        
        f.seek(0, 2) # Determine file size
        file_size = f.tell()
        
        # Повторное чтение лога при изменении размера файла
        if start_offset > file_size: 
            start_offset = 0 

        f.seek(start_offset) # ищем точку начала чтения
        last_offset = start_offset
        
        progress = None
        
        # display the progress
        if show_progress:
            progress = tqdm(
                total=file_size,
                initial=start_offset,
                unit='B',
                unit_scale=True,
                desc=f"Processing {os.path.basename(file_path)}",
                dynamic_ncols=True
            )
       
        while True:
            line = f.readline()
            if not line:
                break

            current_offset = f.tell()
            if progress:
                progress.update(current_offset - last_offset)
           
            last_offset = current_offset
            yield line, current_offset
        
        if progress:
            progress.close()
