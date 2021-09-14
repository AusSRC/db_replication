import psycopg2
import datetime
import time
import unittest
import logging,sys,csv
from database_replicator import DatabaseReplicator


class TestReplicationBenchmarking(unittest.TestCase):
    """A collection of tests for benchmarking write/update query replication.

    """
    def setUp(self):
        """Create the local and replica databases.

        """
        self.dbr = DatabaseReplicator()
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

        

    def test_benchmark_100_insert(self):
        """
        """

        id_sync = 102
        operation = "insert"

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

            time.sleep(1)
        
        # Get results of the sync delays
        cur_bucardo.execute(
                """
                SELECT started, ended, inserts FROM syncrun WHERE ended IS NOT NULL and inserts = %i;
                """ % (id_sync)
            )    
        result = cur_bucardo.fetchone()

        output = {'started':result[0], 
                  'ended':result[1], 
                  'rows':result[2],
                  'operation': operation}
                
        with open(r'results.csv', 'a', newline='') as csvfile:
            fieldnames = ['started','ended','rows','operation']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(output)


        cur_master.close()
        conn_master.close()
        cur_bucardo.close()
        conn_bucardo.close()

