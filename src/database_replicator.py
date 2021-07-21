import asyncio
import asyncpg
import psycopg2


class DatabaseReplicator:
    """Database replication class. Used for multi-master experiment with Bucardo (which
    is assumed to be running).

    """
    def __init__(self):
        self.master = []
        self.replica = []

    def add_master(self, host, database, user, password, port="5432"):
        """Provide credentials for a master database.

        """
        credentials = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port,
        } 
        psycopg2.connect(**credentials)
        self.master.append(credentials)

    def add_replica(self, host, database, user, password, port="5432"):
        """Add a replica database.

        """
        credentials = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port,
        } 
        psycopg2.connect(**credentials)
        self.replica.append(credentials)

    async def execute_in_master(self, query):
        """Submit a query to all master database instances.

        """
        for creds in self.master:
            try:
                conn = await asyncpg.connect(**creds)
                await conn.execute(query)
            finally:
                await conn.close()
