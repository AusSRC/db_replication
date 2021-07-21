import unittest
from database_replicator import DatabaseReplicator


class TestOneDirectionalReplication(unittest.IsolatedAsyncioTestCase):
    """Makes use of DatabaseReplicator class. Since instance of that class
    for this test case.

    """

    def setUp(self):
        """Create the local and replica databases.

        """
        self.dbr = DatabaseReplicator()

    async def test_one_directional_write_success(self):
        """Write some data to the local database and verify that the
        data has been written to remote databases.

        """
        await self.dbr.execute_in_master(
            """
            INSERT INTO 
                wallaby.run(name, sanity_thresholds) 
            VALUES 
                ('test_one_directional_write', '{"flux": 10, "spatial_extent": [10, 10], "spectral_extent": [10, 10], "uncertainty_sigma": 5}')
            """
        )