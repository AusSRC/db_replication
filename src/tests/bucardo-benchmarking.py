import psycopg2
import datetime
import time
import unittest
from database_replicator import DatabaseReplicator


class TestReplicationBenchmarking(unittest.TestCase):
    """A collection of tests for benchmarking write/update query replication.

    """
    def setUp(self):
        """Create the local and replica databases.

        """
        self.dbr = DatabaseReplicator()
        self.dbr.add_master(
            host="localhost", 
            database="sofiadb",
            user="postgres",
            password=""
        )
        self.dbr.add_bucardo(
            host="localhost",
            database="bucardo",
            user="postgres",
            password="",
            port=""
        )

    def test_benchmark_100_insert(self):
        """
        """
        id_sync = 100
        duration = 0.0
        timeout = 1200.0

    
        # connect to master and bucardo db
        conn_master = psycopg2.connect(**self.dbr.master[0])
        cur_master = conn_master.cursor()
        conn_bucardo = psycopg2.connect(**self.dbr.bucardo[0])
        cur_bucardo = conn_bucardo.cursor()

        # Get inserts sentences
        with open('100_insert.sql') as f:
            contents = f.read()
    
        # Execute SQL bundle
        cur_master.execute(
            """
            %s
            """ % (contents)
        )
        cur_master.fetchone()
        conn_master.commit()

        # Wait until sync sequence start posting data on the ended column
        dt = datetime.datetime.now()
        while duration < timeout:
            # Execute query on Bucardo DB.
            cur_bucardo.execute(
                """
                SELECT * FROM syncrun WHERE ended IS NULL;
                """
            )
            result = cur_master.fetchone()
            if result is not None:
                duration = timeout

            time.sleep(1)
            duration = (datetime.datetime.now() - dt).total_seconds()
        
        # Get results of the sync delays
        cur_bucardo.execute(
                """
                SELECT started, ended, inserted FROM syncrun WHERE ended IS NOT NULL and inserts == %i;
                """ % (id_sync)
            )    
        result = cur_master.fetchone()
        started, ended, inserted  = result[0], result[1], result[2]

        # Results will go to a CSV file

        cur_master.close()
        conn_master.close()
        cur_bucardo.close()
        conn_bucardo.close()
