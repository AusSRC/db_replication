import datetime
import time
import unittest
from database_replicator import DatabaseReplicator


class TestAsyncReplicationBenchmarking(unittest.IsolatedAsyncioTestCase):
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

    async def test_one_directional_write_run_benchmark(self):
        """Test how long it takes to write a run to the replica.

        """
        update_id = 184
        
        # Add trigger to replica
        await self.dbr.query_replica(
            """
            CREATE OR REPLACE FUNCTION log_run_replication()
                RETURNS TRIGGER 
                LANGUAGE PLPGSQL
                AS
            $$
            BEGIN
                UPDATE
                    wallaby.run
                SET 
                    name=to_char(now(), 'YYYY-MM-DD_HH24:MI:SS')
                WHERE 
                    id=%i;
                
                RETURN NEW;
            END;
            $$
            """ % (update_id)
        )
        await self.dbr.query_replica(
            """
            CREATE TRIGGER run_trigger
            AFTER INSERT ON 
                wallaby.run
            EXECUTE PROCEDURE 
                log_run_replication();
            """
        )

        # Write to master
        dt = datetime.datetime.now()
        name = f"benchmarking_run_write_{dt.strftime('%H:%M:%S_%m/%d/%y')}"
        result = await self.dbr.fetchrow_master(
            """
            INSERT INTO 
                wallaby.run(name, sanity_thresholds) 
            VALUES 
                ('%s', '{"flux": 10, "spatial_extent": [10, 10], "spectral_extent": [10, 10], "uncertainty_sigma": 5}')
            RETURNING id
            """ % (name)
        )
        run_id = result[0]['id']
        print(run_id)

        # Wait
        time.sleep(3)

        result = await self.dbr.fetchrow_replica(
            """
            SELECT name FROM wallaby.run WHERE id=%i
            """ % (run_id)
        )
        print(result)

        # Check replica table
        result = await self.dbr.fetchrow_replica(
            """
            SELECT name FROM wallaby.run WHERE id=%i
            """ % (update_id)
        )

        # Calculate time
        print(result[0]['name'])

        # Delete trigger
        await self.dbr.query_replica(
            """
            DROP TRIGGER run_trigger ON wallaby.run;
            """
        )
        