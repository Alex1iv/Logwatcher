import time
import yaml
import json
from pathlib import Path
from glob import glob
import os
from tqdm import tqdm
import re 

from parser import parse_log_line
from postgres_writer import DBWriter
from datetime import datetime, timedelta
import logging

STATE_FILE = "state/file-offsets.json"


def is_recent_event(event, max_age_days: int):
    # print(f"max_age_days value: {max_age_days}")
    # print(f"max_age_days type: {type(max_age_days)}")

    cutoff = datetime.now() - timedelta(days=max_age_days)
    return event["timestamp"] >= cutoff


def load_state():
    """Загружаем точки остановки чтения файлов
    """    
    if not Path(STATE_FILE).exists():
        print("Точки остановки не загружены")
        #raise ValueError("Точки остановки не загружены") # No state was provided!
        return {}
    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state: dict):
    Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def discover_files(log_dir:str, file_pattern:str, logger=None):
    """Discover  log files using regex from config.
    """    
    try:
        regex = re.compile(file_pattern)
    except re.error as e:
        raise ValueError(f"Invalid file_name_regex: {file_pattern}") from e

    matched_files = []

    for name in os.listdir(log_dir):
        if regex.match(name):
            full_path = os.path.join(log_dir, name)
            matched_files.append(full_path)

    if logger:
        logger.info(f"Matched log files: {matched_files}")

    return matched_files

def read_new_lines_with_progress(file_path: str, start_offset: int):
    """
    Reads file line by line and displays a progress bar.
    Returns (new_offset, list_of_lines)
    """
    
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        # Determine the correct starting point
        f.seek(0, 2) # Determine file size
        file_size = f.tell()

        if start_offset > file_size: # Handle log rotation / truncation
            start_offset = 0 

        f.seek(start_offset)

        # display the progress
        progress = tqdm(
            total=file_size,
            initial=start_offset,
            unit='B',
            unit_scale=True,
            desc=f"Processing {os.path.basename(file_path)}",
            dynamic_ncols=True
            #leave=False
        )
       
        while True:
            line = f.readline()
            if not line:
                break

            current_offset = f.tell()
            progress.update(current_offset - progress.n)

            yield line, current_offset

        progress.close()



def get_logger(path, file):
    """Логгирование
    """    
    os.makedirs(path, exist_ok=True)
    log_file = os.path.join(path, file)

    logging.basicConfig(
        level= logging.INFO, #logging.WARNING, #
        format="%(levelname)s: %(asctime)s: %(message)s"
    )

    logger = logging.getLogger()
    handler = logging.FileHandler(log_file, encoding='utf-8')
    handler.setFormatter(logging.Formatter(
        "%(levelname)s: %(asctime)s: %(message)s"
    ))
    logger.addHandler(handler)

    return logger



def main():
    #print("start file")
    logger=None
    
    # Load config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    if config.get("logging", False):
        logger = get_logger(path="logs", file="data.logs")

    MAX_AGE_DAYS = config.get("max_event_age_days", 5)
    SAVE_OFFSET_EVERY_LINES = config.get("save_offset_every_lines", 1000)

    interval = config["parse_interval_seconds"]
    log_dir = config["log_dir"]
    pattern = config["file_pattern"]

    state = load_state()

    #print(f"[START] Watching directory: {log_dir}")

    if logger:
        logger.info(f"Watching directory: {log_dir}")
        logger.info(f"Pattern: {pattern}")
        logger.info(f"Loaded state: {state}")

    db = DBWriter(config, logger)

    while True:

        files = discover_files(log_dir, pattern, logger)
        all_events = []

        for file_path in files:
            offset = state.get(file_path, 0)
            #print(f"[FILE] {file_path} offset={offset}")
            
            if logger:
                logger.info(f"Processing: {file_path}")
                logger.info(f"Previous offset: {offset}")

            lines_processed = 0
            last_offset = offset

            for line, current_offset in read_new_lines_with_progress(file_path, offset):

                lines_processed += 1
                last_offset = current_offset

                parsed = parse_log_line(line)
                                    
                if parsed:
                    if parsed and is_recent_event(parsed, MAX_AGE_DAYS):
                        all_events.append(parsed)

                # Save offset periodically
                if lines_processed % SAVE_OFFSET_EVERY_LINES == 0:
                    state[file_path] = last_offset
                    save_state(state)

                    if logger:
                        logger.info(f"Progress saved for {file_path} at offset {last_offset}")


            # Final offset save for this file
            state[file_path] = last_offset

        # Store events
        if all_events:
            if logger:
                logger.info(f"Parsed {len(all_events)} events")
            db.insert_events(all_events)
        else:
            if logger:
                logger.info("No new events")

        # Persist state at end of cycle
        save_state(state)

        time.sleep(interval)

if __name__ == "__main__":
    main()