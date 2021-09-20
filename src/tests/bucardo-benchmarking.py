import psycopg2
import datetime
import time
import unittest
import logging,sys,csv
from parameterized import parameterized
from database_replicator import DatabaseReplicator
from benchmark_utils import BenchmarkUtils


class TestReplicationBenchmarking(unittest.TestCase):
    """A collection of tests for benchmarking write/update query replication.

    """

    # Paramaterized expansions to generate test cases on-line

    
    # Connection setup
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

    # Decorator
    @parameterized.expand([
        ["insert/delete", 100],
        ["insert/delete", 200],
        ["insert/delete",1000],
    ])
    def test_A_benchmark_wallaby_run (self,operation,sequence):
        if operation == "insert/delete":
            self.run_insert(sequence)
            self.run_delete(sequence)
            self.assertTrue(True)

    
    def run_insert(self, sequence):
        """ 
        """

        id_sync = sequence
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
            time.sleep(3)
        
        # Get results of the sync delays
        cur_bucardo.execute(
                "SELECT started, ended, inserts FROM syncrun WHERE ended IS NOT NULL order by started DESC;"
            )    
        result = cur_bucardo.fetchone()
        
        self.bu.addStats(row=result, nrows=id_sync, operation=operation)
        
        cur_master.close()
        conn_master.close()
        cur_bucardo.close()
        conn_bucardo.close()

    def run_delete(self,sequence):
        """
        """

        time.sleep(3)

        id_sync = sequence
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
            time.sleep(3)
        
        # Get results of the sync delays
        cur_bucardo.execute("SELECT started, ended, deletes FROM syncrun WHERE ended IS NOT NULL order by started DESC;")    
        result = cur_bucardo.fetchone()
        
        self.bu.addStats(row=result, nrows=id_sync, operation=operation)
        
        cur_master.close()
        conn_master.close()
        cur_bucardo.close()
        conn_bucardo.close()

