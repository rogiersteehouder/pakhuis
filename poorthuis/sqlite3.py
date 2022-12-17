import sqlite3
from functools import lru_cache

# Uses a sqlite database to lookup the hash
#
# create table ACCOUNTS (USERNAME text unique, PWD_HASH text);


class SqliteAccounts:
    """Accounts in a sqlite database"""

    def __init__(
        self,
        database_file: str,
        table_name: str = "ACCOUNTS",
        user_field: str = "USERNAME",
        hash_field: str = "PWD_HASH",
    ):
        self.sql = f"select {hash_field} from {table_name} where {user_field} = ?"
        self.db_file = database_file
        # use lru_cache for efficiency
        # used like this to prevent memory leak (cache retains reference to instance)
        self.__call__ = lru_cache(maxsize=10)(self.__call__)

    def __call__(self, username: str) -> str:
        """Password hash for user"""
        if self.last_lookup[0] != username:
            conn = sqlite3.connect(self.db_file)
            row = conn.execute(self.sql, (username,)).fetchone()
            conn.close()
            if row and row[0]:
                self.last_lookup = (username, row[0])
            else:
                self.last_lookup = (username, None)
        return self.last_lookup[1]
