import psycopg2


class DatabaseReplicator:
    """Database replication class. Used for multi-master experiment with Bucardo (which
    is assumed to be running).

    """
    def __init__(self):
        self.local = []
        self.replica = []

    def connect_local(self, host, database, user, password, port=5432):
        """Provide credentials for a local database (master)

        """
        psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        

    def connect_replica(self, host, database, user, password, port=5432):
        """Add a replica database.

        """
        pass