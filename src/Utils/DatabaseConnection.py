import pyodbc
import time
from settings.DataBaseSetting import DATABASE_CONNECTION_STRING


class DatabaseConnection:
    _instance = None  

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:   
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self):
         
        if not hasattr(self, "_initialized"):
            self.max_retries = 3
            self.retry_delay = 0.3
            self._initialized = True

    def connection(self, retry_count=0):
        """
        Devuelve una conexión a la base de datos. Implementa reintentos en caso de fallo.
        """
        try:
            connection = pyodbc.connect(DATABASE_CONNECTION_STRING)
            print('hecho')
            return connection
        except Exception as e:
            print(str(e))
            if retry_count < self.max_retries:
                time.sleep(self.retry_delay * (retry_count + 1))   
                return self.connection(retry_count=retry_count + 1)
            else:
                return None
