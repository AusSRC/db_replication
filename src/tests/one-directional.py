import time
import datetime
import string
import random
import unittest
from database_replicator import DatabaseReplicator


class TestAsyncOneDirectionalReplication(unittest.IsolatedAsyncioTestCase):
    """Makes use of DatabaseReplicator class. Since instance of that class
    for this test case.

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
        self.dbr.add_replica(
            host="161.111.167.192",
            database="sofiadb",
            user="postgres",
            password="",
            port="18020"
        )

    async def test_one_directional_write_success(self):
        """Write some data to the local database and verify that the
        data has been written to remote databases.

        """
        # Write to master
        name = f"test_one_directional_write_{datetime.datetime.now().strftime('%H:%M:%S_%m/%d/%y')}"
        await self.dbr.query_master(
            """
            INSERT INTO 
                wallaby.run(name, sanity_thresholds) 
            VALUES 
                ('%s', '{"flux": 10, "spatial_extent": [10, 10], "spectral_extent": [10, 10], "uncertainty_sigma": 5}')
            """ % (name)
        )

        # Wait
        time.sleep(2)

        # Check replica table
        result = await self.dbr.query_replica(
            """
            SELECT * FROM wallaby.run WHERE name='%s'
            """ % (name)
        )

        # Assert
        for res in result:
            _, n_rows = res.split(" ")
            self.assertEqual(int(n_rows), 1)

    async def test_one_directional_update_success(self):
        """UPDATE query on the master database should be reflected in the replica.
        Set the name a run to a random 20 letter string.

        """
        # Update name in master
        name = ''.join(random.choice(string.ascii_letters) for _ in range(20))
        await self.dbr.query_master(
            """
            UPDATE 
                wallaby.run
            SET 
                name='%s'
            WHERE
                id='140'
            """ % (name)
        )

        # Wait
        time.sleep(2)

        # Check replica table
        result = await self.dbr.query_replica(
            """
            SELECT * FROM wallaby.run WHERE name='%s'
            """ % (name)
        )

        # Assert
        for res in result:
            _, n_rows = res.split(" ")
            self.assertEqual(int(n_rows), 1)
