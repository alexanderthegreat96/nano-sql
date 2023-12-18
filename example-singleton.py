from nanosql import NanoSql
import mysql.connector as mysql
import time
class Database:
    # force a single initialization only

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance.db = None
            cls._instance.__connect()
        return cls._instance
    
    def __init__(self):
        self.now = time.strftime('%Y-%m-%d %H:%M:%S')

    def __connect(self):
        try:
            if self._instance.db == None:
                self._instance.db = NanoSql(
                host="localhost",
                db="my-db",
                user="my-user",
                passwd="my-pass",
                port=3306,
                pool_size = 8
                )
            else:
                print("INFO: MySQL Connection already established. Canceling further connections!")
        except mysql.OperationalError as e:
            # Reconnect if the connection is lost
            if e.errno == 2013:  # Check for specific error code 2013
                print("INFO: Lost connection to the database. Reconnecting...")
                self.__connect()
            else:
                # Handle other OperationalError exceptions as needed
                print(f"INFO: OperationalError executing query: {str(e)}")
                raise
        except Exception as e:
            # Handle other exceptions as needed
            print(f"INFO: Error executing query: {str(e)}")
            raise
    