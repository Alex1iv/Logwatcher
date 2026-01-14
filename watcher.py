import yaml
from pathlib import Path
import pandas as pd
# import os
import re 

from parser import parse_log_line, parse_arguments
from postgres_writer import DBWriter
from logreader import is_recent_event, load_state, \
    save_state, discover_files, read_new_lines_with_progress
from logging_utils import get_logger
from downloader import download_csv
from cred_loader import load_credentials

def update_ports_from_csv(db:DBWriter, url_ports:str, credentials:dict, 
    ports_path:str, logger):
    """Обновление портов в бд ports. Если файл не загрузился,
        то обновления бд не выполянется.

    Args:
        db (DBWriter): обработчик бд
        paths (dict): пути к файлам
        credentials (dict): логин и пароль к серверу загрузки
        ports_path (str): локальная папка загрузки файла ports.csv
        logger (_type_): логгер
    """        
    if not download_csv(url_ports, credentials, ports_path, logger):
        return 

    df = pd.read_csv(
        ports_path,
        sep=";",
        names=[
            "ip", "letter", "switch", "port",
            "col_1", "col_2", "col_3", "col_4",
            "col_5", "col_6", "col_7", "col_8", "col_9"
        ],
    )

    port_pattern = re.compile(r"(?<!^)[.]")
    df["is_magistral"] = df["port"].apply(
        lambda x: 1 if re.search(port_pattern, str(x)) else 0
    )

    ports = df[["ip", "letter", "switch", "port", "is_magistral"]] \
        .to_dict(orient="records")

    db.ensure_ports_table()
    db.refresh_ports(ports)

  
def main():
    args = parse_arguments() # run with arguments

    BASE_DIR = Path("D:/IDE/Logwatcher/") if args.remote else Path("/usr/share/logwatcher/")
    
    # Load config
    with open(Path(BASE_DIR, "config.yaml")) as f:
        config = yaml.safe_load(f)
    
    paths = config["remote"] if args.remote else config["local"] 
    
    MONITORING_DIR = paths["MONITORING_DIR"]
    file_pattern = paths["FILE_PATTERN"]
    MAX_AGE_DAYS = config.get("max_event_age_days", 5)
    SAVE_OFFSET_EVERY_LINES =   config.get("SAVE_OFFSET_EVERY_LINES", 1000)
    PORTS_PATH = str(BASE_DIR / paths["PORTS_PATH"])
    STATE_FILE = BASE_DIR / paths["STATE_FILE"]
    
    if config.get("logging", False):
        logger = get_logger(path=paths["LOG_DIR"], file="data.logs")
 
    # Загрузка логинов и паролей
    credentials = load_credentials(args, paths, logger)
    
    # загрузка точек остановки чтения файлов
    state = load_state(STATE_FILE)
    
    # функции обработки бд Postgres
    db = DBWriter(config, credentials, logger)
        
    # Загружаем список портов в базу
    update_ports_from_csv(db, config.get("url_ports"), credentials, PORTS_PATH, logger)
        
    files = discover_files(MONITORING_DIR, file_pattern, logger)
    all_events = []

    for file_path in files:
        file_key = str(file_path)
        offset = state.get(file_key, 0)
        lines_processed = 0
        last_offset = offset

        # читаем строки логов
        for line, current_offset in read_new_lines_with_progress(
            file_path, offset, show_progress=False):

            lines_processed += 1
            last_offset = current_offset

            parsed = parse_log_line(line)
                                
            if parsed:
                # парсим только недавние события
                if parsed and is_recent_event(parsed, MAX_AGE_DAYS):
                    all_events.append(parsed)

            # Сохраняем прогресс чтения файлов периодически
            if lines_processed % SAVE_OFFSET_EVERY_LINES == 0:
                state[file_key] = last_offset
                save_state(STATE_FILE, state) # не логгируем промежуточные изменения
                #save_state(STATE_FILE, state, logger, file_path=str(file_path), offset=last_offset)

        # Final offset save for this file
        state[file_key] = last_offset

    # Store events
    if all_events:
        db.insert_events(all_events)
        
    else:
        if logger:
            logger.info("No new events")

    # Persist state at end of cycle
    save_state(STATE_FILE, state, logger, 
        file_path=str(paths["LOG_DIR"]), offset=last_offset)



if __name__ == "__main__":
    main()
