import psycopg2
import datetime
import time
import unittest
import logging,sys,csv
from parameterized import parameterized
from database_replicator import DatabaseReplicator
from benchmark_utils import BenchmarkUtils

# Size of cubes 
MB_05=1024*1024*0.5 # 512KB
MB_1=1024*1024*1
MB_2=1024*1024*2
MB_5=1024*1024*5
MB_10=1024*1024*10 # 10MB
MB_20=1024*1024*20 # 20MB
MB_30=1024*1024*30 # 30MB
MB_30=1024*1024*40 # 40MB
MB_50=1024*1024*50 # 50MB

class TestReplicationBenchmarking(unittest.TestCase):
    """A collection of tests for benchmarking write/delete query replication.

    """
    
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

    # Decorator to include the stack of runs, for this: 
    @parameterized.expand([
    ["insert/delete", 2,  MB_05, 10],
    ["insert/delete", 5,  MB_05, 10],
    ["insert/delete", 10, MB_05, 10],
    ["insert/delete", 15, MB_05, 10],
    ["insert/delete", 20, MB_05, 10],
    ["insert/delete", 25, MB_05, 10],
    ["insert/delete", 30, MB_05, 10],
    ["insert/delete", 35, MB_05, 10],
    ["insert/delete", 40, MB_05, 10],
    ["insert/delete", 45, MB_05, 10],
    ["insert/delete", 50, MB_05, 10],
    ["insert/delete", 60, MB_05, 10],
    ["insert/delete", 70, MB_05, 10],
    ["insert/delete", 80, MB_05, 10],
    ["insert/delete", 90, MB_05, 10],
    ["insert/delete", 100,MB_05, 10]

    ["insert/delete", 2,  MB_2, 10],
    ["insert/delete", 5,  MB_2, 10],
    ["insert/delete", 10, MB_2, 10],
    ["insert/delete", 15, MB_2, 10],
    ["insert/delete", 20, MB_2, 10],
    ["insert/delete", 25, MB_2, 10],
    ["insert/delete", 30, MB_2, 10],
    ["insert/delete", 35, MB_2, 10],
    ["insert/delete", 40, MB_2, 10],
    ["insert/delete", 45, MB_2, 10],
    ["insert/delete", 50, MB_2, 10],
    ["insert/delete", 60, MB_2, 10],
    ["insert/delete", 70, MB_2, 10],
    ["insert/delete", 80, MB_2, 10],
    ["insert/delete", 90, MB_2, 10],
    ["insert/delete", 100,MB_2, 10],

    ["insert/delete", 2,  MB_5, 10],
    ["insert/delete", 5,  MB_5, 10],
    ["insert/delete", 10, MB_5, 10],
    ["insert/delete", 15, MB_5, 10],
    ["insert/delete", 20, MB_5, 10],
    ["insert/delete", 25, MB_5, 10],
    ["insert/delete", 30, MB_5, 10],
    ["insert/delete", 35, MB_5, 10],
    ["insert/delete", 40, MB_5, 10],
    ["insert/delete", 45, MB_5, 10],
    ["insert/delete", 50, MB_5, 10],
    ["insert/delete", 60, MB_5, 10],
    ["insert/delete", 70, MB_5, 10],
    ["insert/delete", 80, MB_5, 10],
    ["insert/delete", 90, MB_5, 10],
    ["insert/delete", 100,MB_5, 10],

    ["insert/delete", 2,  MB_10, 10],
    ["insert/delete", 5,  MB_10, 10],
    ["insert/delete", 10, MB_10, 10],
    ["insert/delete", 15, MB_10, 10],
    ["insert/delete", 20, MB_10, 10],
    ["insert/delete", 25, MB_10, 10],
    ["insert/delete", 30, MB_10, 10],
    ["insert/delete", 35, MB_10, 10],
    ["insert/delete", 40, MB_10, 10],
    ["insert/delete", 45, MB_10, 10],
    ["insert/delete", 50, MB_10, 10]
    
    ])
    def test_A_benchmark_wallaby_testfattable (self,operation,sequence,product_size,times):
        """Unit Test for the Wallaby.test_fattable table.
        Check table template to implement new data generation for it
        
        """
        if operation == "insert/delete":
            # Each operation for us is a atomic set of insert and update           
            for i in range(0,times):
                self.run_insert_wallaby_testfattable(sequence,product_size)
                time.sleep(60)
                self.run_delete_wallaby_testfattable(sequence,product_size)
                time.sleep(60)
            
            self.assertTrue(True)    
     
    
    def run_insert_wallaby_testfattable(self,sequence,product_size):
        """ Generic insert function.
        Check tables templates to add new tables to insert data following a schema.
        """
        time.sleep(5)
        
        id_sync = sequence
        operation = "insert"

        duration = 0.0
        timeout = 1500.0

        # connect to master and bucardo db
        conn_master = psycopg2.connect(**self.dbr.master[0])
        cur_master = conn_master.cursor()
        conn_bucardo = psycopg2.connect(**self.dbr.bucardo[0])
        cur_bucardo = conn_bucardo.cursor()

        # Build batch sentences
        self.bu.buildBatch(nrows=id_sync, table = "wallaby.test_fattable", operation = "insert", product_size=product_size )
    
        # Get batch sentences
        content = self.bu.getBatch(nrows=id_sync, table = "wallaby.test_fattable", operation = "insert", product_size=product_size)    
   
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
        
        self.bu.addStats(row=result, nrows=id_sync, operation=operation, table="wallaby.test_fattable", product_size=product_size)
        
        cur_master.close()
        conn_master.close()
        cur_bucardo.close()
        conn_bucardo.close()

    def run_delete_wallaby_testfattable(self,sequence,product_size):
        """ Generic delete function.
        Check tables templates to add new tables to insert data following a schema.
        """

        # Sometimes Bucardo syncdb table is not populated so fast.
        time.sleep(5)

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
        self.bu.buildBatch(nrows=id_sync, table = "wallaby.test_fattable", operation = "delete", product_size=product_size)
    
        # Get batch sentences
        content = self.bu.getBatch(nrows=id_sync, table = "wallaby.test_fattable", operation = "delete", product_size=product_size)    
   
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
        
        self.bu.addStats(row=result, nrows=id_sync, operation=operation, table="wallaby.test_fattable")
        
        cur_master.close()
        conn_master.close()
        cur_bucardo.close()
        conn_bucardo.close()

