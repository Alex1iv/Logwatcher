import re
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from downloader import download_csv

class DBWriter:
    """Экспорт данных в Postgresql
    """    
    def __init__(self, config, credentials:dict, logger=None):
        """_summary_

        Args:
            config (_type_): подключение к бдЖ хост, порт, название бд
            credentials (dict): логин и пароль
            logging (_type_, optional): логгер. Defaults to None.
        """        
        config_db = config["db"]
        self.conn = None

        try:
            self.conn = psycopg2.connect(
                host = config_db["host"],
                port = config_db["port"],
                dbname = config_db["dbname"],
                user = credentials["POSTGRES_USER"],
                password = credentials["POSTGRES_PASSWORD"]
            )
            self.conn.autocommit = True
            self.logger = logger if config["logging"] else None
            
            if self.logger:
                logger.info(f"[DB] Connected successfully")

        except Exception as e:
            print("[DB] ERROR connecting to database:")
            print(str(e))
            raise

    def insert_events(self, events):
        if not events:
            return

        try:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO log_events (
                        timestamp,
                        device_ip,
                        error_type,
                        mac,
                        vlan,
                        src,
                        dst,
                        raw_line
                    )
                    VALUES %s
                    """,
                    [
                        (
                            e["timestamp"],
                            e["device_ip"],
                            e["error_type"],
                            e["mac"],
                            e["vlan"],
                            e["src"],
                            e["dst"],
                            e["raw_line"],
                        )
                        for e in events
                    ]
                )
            
            if self.logger:
                self.logger.info(f"[DB] Inserted {len(events)} events")
            
            #print(f"[DB] Inserted {len(events)} events")

        except Exception as e:
            # print("[DB] ERROR inserting events:")
            # print(str(e))
            if self.logger:
                self.logger.info(f"[DB] ERROR inserting events")
            raise
    
    def ensure_ports_table(self):
        """Загрузка данных портов
        """        
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ports (
                    id SERIAL PRIMARY KEY,
                    ip VARCHAR(15),
                    letter VARCHAR(2),
                    switch VARCHAR(25),
                    port VARCHAR(50),
                    is_magistral INT
                );
            """)
        self.conn.commit()

    def refresh_ports(self, ports):
        """
        загрузка данных о портах
        ports: list[dict]
        """
        # исключаем загрузку пустых строк
        if not ports:
            if self.logger:
                self.logger.info(f"[PORTS] There are no rows to insert")
            return

        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE ports RESTART IDENTITY;")

            execute_values(
                cur,
                """
                INSERT INTO ports (ip, letter, switch, port, is_magistral)
                VALUES %s
                """,
                [
                    (
                        p["ip"],
                        p["letter"],
                        p["switch"],
                        p["port"],
                        p["is_magistral"]
                    )
                    for p in ports
                ]
            )
        self.conn.commit()
        
        if self.logger:
            self.logger.info(f"[PORTS] Loaded {len(ports)} ports into database")
            
