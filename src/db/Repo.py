import mysql.connector as mc
from mysql.connector import Error
import dotenv
import os

class Repo:
    def __init__(self):
        dotenv.load_dotenv()
        self.DB_CONFIG = {
                'host': os.getenv('DB_HOST'),
                'port': int(os.getenv('DB_PORT', 3306)),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'database': os.getenv('DB_DATABASE'),
                'autocommit': False,
        }
        config = {key: self.DB_CONFIG[key] if key != 'password' else '****' for key in self.DB_CONFIG}
        print(f"[INFO] initialize repo: DB_CONFIG={config}")

        self._connect()
        print(f"[INFO] connection success")
    
    
    def select(self, query, params=()):
        cursor = self._get_cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        except Error as e:
            print(f"[Error] Failed to select: query={query}, params={params}")
            raise RuntimeError(e)
        finally:
            cursor.close()

    def update(self, query, params=()):
        cursor = self._get_cursor()
        try:
            cursor.execute(query, params)
            self._commit()
            return cursor.rowcount
        except Error as e:
            self._rollback()
            print(f"[Error] Failed to update: query={query}, params={params}")
            raise RuntimeError(e)
        finally:
            cursor.close()

    def insert(self, query, params=()):
        cursor = self._get_cursor()
        try:
            cursor.execute(query, params)
            self._commit()
            return cursor._last_insert_id
        except Error as e:
            self._rollback()
            print(f"[Error] Failed to insert: query={query}, params={params}")
            raise RuntimeError(e)
        finally:
            cursor.close()

    def _connect(self):
        try:
            self._conn = mc.connect(**self.DB_CONFIG)
        except Error as e:
            print("[Error] Connection Failed")
            raise RuntimeError(e)
    def _get_cursor(self):
        try:
            if not self._conn or not self._conn.is_connected():
                self._connect()
            return self._conn.cursor()
        except Error as e:
            print("[Error] Failed to create cursor")
            raise RuntimeError(e)
    def _commit(self):
        if not self._conn or not self._conn.is_connected():
                self._connect()
        self._conn.commit()
    def _rollback(self):
        if not self._conn or not self._conn.is_connected():
                self._connect()
        self._conn.rollback()