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
            password="postgres"
        )
        self.dbr.add_replica(
            host="161.111.167.192",
            database="sofiadb",
            user="postgres",
            password="AmIgAgR0u;P",
            port="18020"
        )

    def test_one_directional_write_run_single_benchmark(self):
        """Test how long it takes to write a run to the replica.
        For this experiment 

        """
        duration = 0.0
        timeout = 10.0

        try:
            # connect to master
            conn_master = psycopg2.connect(**self.dbr.master[0])
            cur_master = conn_master.cursor()

            try:
                # connect to replica
                conn_replica = psycopg2.connect(**self.dbr.replica[0])
                cur_replica = conn_replica.cursor()

                # write to master
                name = f"benchmarking_run_write_{datetime.datetime.now().strftime('%H:%M:%S_%m/%d/%y')}"
                cur_master.execute(
                    """
                    INSERT INTO 
                        wallaby.run(name, sanity_thresholds) 
                    VALUES 
                        ('%s', '{"flux": 10, "spatial_extent": [10, 10], "spectral_extent": [10, 10], "uncertainty_sigma": 5}')
                    RETURNING id
                    """ % (name)
                )
                result = cur_master.fetchone()
                conn_master.commit()
                run_id = result[0]
                dt = datetime.datetime.now()

                # constantly check replica
                while duration < timeout:
                    cur_replica.execute(
                        """
                        SELECT * FROM wallaby.run WHERE id=%i
                        """ % (run_id)
                    )
                    result_replica = cur_replica.fetchone()
                    if result_replica is not None:
                        time = round((datetime.datetime.now() - dt).total_seconds(), 3)
                        print(f"Replication time = {time} seconds")
                        break
                    duration = (datetime.datetime.now() - dt).total_seconds()

            finally:
                cur_replica.close()
                conn_replica.close()
        
        finally:
            cur_master.close()
            conn_master.close()
