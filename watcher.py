import time
import yaml
import json
from pathlib import Path
from glob import glob
import os 

from parser import parse_log_line
from postgres_writer import DBWriter
from datetime import datetime, timedelta
import logging 

STATE_FILE = "state/file-offsets.json"



def is_recent_event(event, max_age_days:int):
    """Читать логи, которые произошли не позже, чем
    """    
    cutoff = datetime.now() - timedelta(days=max_age_days)
    return event["timestamp"] >= cutoff


def load_state():
    """Загрузка точек остановок чтения файлов логов.
    """    
    if not Path(STATE_FILE).exists():
        raise ValueError("No state was provided!")
        #return {}
        
    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state:dict):
    """Сохранение точек остановок чтения файлов логов.
    """
    Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def tail_file(file_path:str, offset:int):
    """
    Читаем файл с указанного места. Возвращает точку остановки и список новых строк.
    """
    lines = []
    with open(file_path, "r") as f:
        # If file was rotated and shrank, reset offset
        f.seek(0, 2)
        size = f.tell()
        if offset > size:
            offset = 0

        f.seek(offset)
        for line in f:
            lines.append(line)

        new_offset = f.tell()

    return new_offset, lines


def discover_files(log_dir, file_pattern, logger=None):
    """
    Returns full paths of all matching log files.
    """
    #pattern = str(Path(log_dir) / file_pattern)
    #return [Path(p).resolve().as_posix() for p in glob(pattern)]
    
    pattern = os.path.join(log_dir, file_pattern)
    
    if logger:
        logger.info(f"Looking for logs at: {pattern}")
        
    files = glob(pattern)

    if logger:
        logger.info(f"Found files: {files}")
        
    return files


# Функция для создания лог-файла и записи в него информации
def get_logger(path, file):
    """[Создает лог-файл для логирования в него]
    Arguments:
        path {string} -- путь к директории
        file {string} -- имя файла
    Returns:
        [obj] -- [логер]
    """
    if not path:
        os.makedirs(path)
    
    # проверяем, существует ли файл
    log_file = os.path.join(path, file)
    
    #если  файла нет, создаем его
    if not os.path.isfile(log_file):
        open(log_file, "w+").close()
    
    # поменяем формат логирования
    file_logging_format = "%(levelname)s: %(asctime)s: %(message)s"
    
    # конфигурируем лог-файл
    logging.basicConfig(level=logging.INFO, 
    format = file_logging_format)
    logger = logging.getLogger()
    
    # хэнлдер для записи лога в файл
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(file_logging_format)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    
    return logger

def main(logger=None):
    
    # Load config
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

    except FileNotFoundError:
        print(f"Config file not found: {file_path}")
        # if logger:
        #     logger.error(f"Unable to load config file.")

    if config.get("logging", False):
        logger = get_logger(path="logs/", file="data.logs") # логгер
        
    MAX_AGE_DAYS = config.get("max_event_age_days", 5)
    
    interval = config["parse_interval_seconds"]
    log_dir = config["log_dir"]
    pattern = config["file_pattern"]

    state = load_state()
    
    if logger:
        logger.info(f"Watching directory: {log_dir}")
        logger.info(f"Pattern: {pattern}")
        logger.info(f"Loaded state: {state}")
       
    db = DBWriter(config, logger)
    
    while True:
        # Discover files dynamically every cycle
        files = discover_files(log_dir, pattern)
               
        all_events = []

        for file_path in files:
            offset = state.get(file_path, 0)
            
            if logger:
                logger.info(f"Processing: {file_path}")
                logger.info(f"Previous offset: {offset}")
            
            try:
                new_offset, lines = tail_file(file_path, offset) # читаем файл с точки остановки
              
            except FileNotFoundError:
                print(f"File not found: {file_path}")
                if logger:
                    logger.error(f"File not found: {file_path}")
                continue
   
            if logger:
                logger.info(f"Lines read: {len(lines)}")
                
            if len(lines) == 0:
                # print("[FILE] No new lines to parse.")
                if logger:
                    logger.info("No new lines to parse.")
            else:
                # print("[FILE] New lines:")
                for l in lines:
                    print("   RAW:", repr(l))

            state[file_path] = new_offset

            # Parse new lines
            for line in lines:
                # print(i)
                parsed = parse_log_line(line)
                
                if parsed:
                    # оставляем ошибки, произошедшие ранее MAX_AGE_DAYS дней
                    if is_recent_event(parsed, MAX_AGE_DAYS): 
                        all_events.append(parsed)
                    else:
                        print(
                            f"Event older than {MAX_AGE_DAYS} days: {parsed['timestamp']}"
                        )
                else:
                    # print("[PARSED NONE]")
                    if logger:
                        logger.info("[PARSED NONE]")
                    pass  
                           

        # Save DB results
        if all_events:
            if logger:
                logger.info(f"Parsed {len(all_events)} events")
            
            db.insert_events(all_events)
        else:
            print("[PARSER] No new events")
            if logger:
                logger.info("[PARSER] No new events")
                
        # Сохраняем точку чтения файла (Persist offsets)
        save_state(state)

        time.sleep(interval) # пауза в чтении логов


if __name__ == "__main__":
    main()
