
import mysql.connector.pooling as mysqlPooling
import mysql.connector.errors

class DbPool(object):

    def __init__(self, db_host, db_port, db_user,
                       db_password, db_database, db_pool_name,
                       db_pool_size):
        res = {}
        self._db_host = db_host
        self._db_port = db_port
        self._db_user = db_user
        self._db_password = db_password
        self._db_database = db_database

        res["host"] = self._db_host
        res["port"] = int(self._db_port)
        res["user"] = self._db_user
        res["password"] = self._db_password
        res["database"] = self._db_database
        self._dbconfig = res

        self._pool = self.create_db_pool(pool_name=db_pool_name, pool_size=db_pool_size)


    def create_db_pool(self, pool_name, pool_size=1):
        db_pool = None
        print(self._dbconfig)
        try:
            db_pool = mysqlPooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=int(pool_size),
                pool_reset_session=True,
                **self._dbconfig
            )
        except mysql.connector.errors.ProgrammingError as e:
            print(str(e))
        except Exception as e:
            print(str(e))

        return db_pool
