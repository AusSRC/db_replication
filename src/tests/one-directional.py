import unittest
from database_replicator import DatabaseReplicator


class TestOneDirectionalReplication(unittest.TestCase):
    def setUp(self):
        """Create the local and replica databases.

        """

    def test_one_directional_write_success(self):
        """Write some data to the local database and verify that the
        data has been written to remote databases.

        """
        pass