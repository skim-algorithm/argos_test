import os
import sqlite3
import pandas as pd

class SqlManager:
    def __init__(self):
        # TODO: 날라가서 다시 짜야해
        # SQLite 데이터베이스 파일명
        self.db_name = "argos_btc.db"
        self.table_name = "argos_data"
        # SQLite 연결 생성
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name):
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (timestamp TEXT PRIMARY KEY, datetime TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)")
        self.conn.commit()

    def insert_data_from_csv(self, csv_folder):
        for file in os.listdir(csv_folder):
            if file.endswith(".csv"):
                file_path = os.path.join(csv_folder, file)
                # CSV 파일 읽기
                df = pd.read_csv(file_path)
                if "timestamp" not in df.columns:
                    print(f"Skipping {file}, no timestamp column found.")
                    continue

                timestamp_col = df["timestamp"]
                datetime_col = df["datetime"]
                open_col = df["open"]
                high_col = df["high"]
                low_col = df["low"]
                close_col = df["close"]
                volume_col = df["volume"]

                for timestamp, datetime, open, high, low, close, volume in zip(timestamp_col, datetime_col, open_col, high_col,
                                                                               low_col, close_col, volume_col):
                    self.cursor.execute(f"INSERT OR IGNORE INTO {self.table_name} (timestamp, datetime, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                   (timestamp, datetime, open, high, low, close, volume))
                self.conn.commit()

    def close_db(self):
        self.conn.close()

sqlmanager = SqlManager()
sqlmanager.create_table("argos_db2")
sqlmanager.insert_data_from_csv("sql_data")
sqlmanager.close_db()