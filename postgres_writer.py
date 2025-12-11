import psycopg2
from psycopg2.extras import execute_values


class DBWriter:
    def __init__(self, config, logging=None):
        config_db = config["db"]
        self.conn = None

        try:
            self.conn = psycopg2.connect(
                host = config_db["host"],
                port = config_db["port"],
                dbname = config_db["dbname"],
                user = config_db["user"],
                password = config_db["password"]
            )
            self.conn.autocommit = True
            self.logging = config["logging"]
            
            if self.logging:
                logging.info(f"Connected successfully")

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
                        timestamp, switch_name, error_type, mac, vlan, 
                        src, dst, raw, created_at
                    )
                    VALUES %s
                    """,
                    [(
                        e["timestamp"],
                        e["switch_name"],
                        e["error_type"],
                        e["mac"],
                        e["vlan"],
                        e["src"],
                        e["dst"],
                        e["raw"],
                        e["created_at"]
                    ) for e in events]
                )

            print(f"[DB] Inserted {len(events)} events")

        except Exception as e:
            print("[DB] ERROR inserting events:")
            print(str(e))
            raise