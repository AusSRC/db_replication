import psycopg2
import datetime
import time
import unittest
import logging,sys,csv
from database_replicator import DatabaseReplicator
from benchmark_utils import BenchmarkUtils


class TestReplicationBenchmarking(unittest.TestCase):
    """A collection of tests for benchmarking write/update query replication.

    """
    def setUp(self):
        """Create the local and replica databases.

        """
        self.dbr = DatabaseReplicator()
        self.bu = BenchmarkUtils()

        self.dbr.add_master(
            host="192.168.100.213", 
            database="sofiadb",
            user="postgres",
            password="",
            port=18020
        )
        self.dbr.add_bucardo(
            host="192.168.100.213",
            database="bucardo",
            user="postgres",
            password="",
            port=18020
        )

        

    def test_A_benchmark_100_insert(self):
        """
        """

        id_sync = 100
        operation = "insert"

        duration = 0.0
        timeout = 1200.0

        # connect to master and bucardo db
        conn_master = psycopg2.connect(**self.dbr.master[0])
        cur_master = conn_master.cursor()
        conn_bucardo = psycopg2.connect(**self.dbr.bucardo[0])
        cur_bucardo = conn_bucardo.cursor()

        # Build batch sentences
        self.bu.buildBatch(nrows=id_sync, table = "wallaby.run", operation = "insert")
    
        # Get batch sentences
        content = self.bu.getBatch(nrows=id_sync, table = "wallaby.run", operation = "insert")    
   
        # Execute SQL bundle
        cur_master.execute(
            """
            %s
            """ % (content)
        )
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
            result = cur_bucardo.fetchone()
            if result is None :
                duration = (datetime.datetime.now() - dt).total_seconds()

            else:
                duration = timeout + 1

            # Wait 1 second to avoid an overflow sending queries
            time.sleep(1)
        
        # Get results of the sync delays
        cur_bucardo.execute(
                "SELECT started, ended, inserts FROM syncrun WHERE ended IS NOT NULL order by ended DESC;"
            )    
        result = cur_bucardo.fetchone()
        
        self.bu.addStats(row=result, operation=operation)
        
        cur_master.close()
        conn_master.close()
        cur_bucardo.close()
        conn_bucardo.close()

        self.assertTrue(True)


    def test_B_benchmark_100_delete(self):
        """
        """

        time.sleep(10)

        id_sync = 100
        operation = "delete"

        duration = 0.0
        timeout = 1200.0

        # connect to master and bucardo db
        conn_master = psycopg2.connect(**self.dbr.master[0])
        cur_master = conn_master.cursor()
        conn_bucardo = psycopg2.connect(**self.dbr.bucardo[0])
        cur_bucardo = conn_bucardo.cursor()

        # Build batch sentences
        self.bu.buildBatch(nrows=id_sync, table = "wallaby.run", operation = "delete")
    
        # Get batch sentences
        content = self.bu.getBatch(nrows=id_sync, table = "wallaby.run", operation = "delete")    
   
        # Execute SQL bundle
        cur_master.execute(
            """
            %s
            """ % (content)
        )
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
            result = cur_bucardo.fetchone()
            if result is None :
                duration = (datetime.datetime.now() - dt).total_seconds()

            else:
                duration = timeout + 1

            # Wait 1 second to avoid an overflow sending queries
            time.sleep(1)
        
        # Get results of the sync delays
        cur_bucardo.execute("SELECT started, ended, deletes FROM syncrun WHERE ended IS NOT NULL order by ended DESC;")    
        result = cur_bucardo.fetchone()
        
        self.bu.addStats(row=result, operation=operation)
        
        cur_master.close()
        conn_master.close()
        cur_bucardo.close()
        conn_bucardo.close()

        self.assertTrue(True)