import asyncio
import asyncpg
import psycopg2


class DatabaseReplicator:
    """Database replication class. Used for multi-master experiment with Bucardo (which
    is assumed to be running on the master and replica instances).

    """
    def __init__(self):
        self.master = []
        self.replica = [] 
        self.bucardo = [] 

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

    def add_bucardo(self, host, database, user, password, port="5432"):
        """Add a bucardo database.

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


    async def query_master(self, query):
        """Submit a query to all master database instances.

        """
        results = []
        for creds in self.master:
            try:
                conn = await asyncpg.connect(**creds)
                res = await conn.execute(query)
                results.append(res)
            finally:
                await conn.close()
        return results

    async def fetchrow_master(self, query, *args, **kwargs):
        """Submit a query to all master database instances.

        """
        results = []
        for creds in self.master:
            try:
                conn = await asyncpg.connect(**creds)
                res = await conn.fetchrow(query, *args, **kwargs)
                results.append(res)
            finally:
                await conn.close()
        return results

    async def query_replica(self, query):
        """Write a query to all replica databases.

        """
        results = []
        for creds in self.replica:
            try:
                conn = await asyncpg.connect(**creds)
                res = await conn.execute(query)
                results.append(res)
            finally:
                await conn.close()
        return results

    async def fetchrow_replica(self, query, *args, **kwargs):
        """Submit a query to all replica database instances.

        """
        results = []
        for creds in self.replica:
            try:
                conn = await asyncpg.connect(**creds)
                res = await conn.fetchrow(query, *args, **kwargs)
                results.append(res)
            finally:
                await conn.close()
        return results

    async def simultaneous_query(self, master_query, replica_query):
        """Submit a query to all databases (master and replicas).

        """
        pass