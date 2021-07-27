import time
import datetime
import string
import random
import unittest
import xmltodict
from utils import read_file, read_json
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
            password="postgres"
        )
        self.dbr.add_replica(
            host="161.111.167.192",
            database="sofiadb",
            user="postgres",
            password="AmIgAgR0u;P",
            port="18020"
        )

    async def test_one_directional_write_run(self):
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

    # NOTE(austin): we use an arbitrary id (140) for the row to update
    async def test_one_directional_update_run(self):
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

    # TODO(austin): Add to the products table for the detection
    async def test_one_directional_write_detection(self):
        """INSERT query for writing a detection to the master database.
        Will write a Run, Instance and Detection row.

        """
        dt = datetime.datetime.now().strftime('%H:%M:%S_%m/%d/%y')
        name = f"test_one_directional_detection_write_{dt}"

        # Write run
        result = await self.dbr.fetchrow_master(
            """
            INSERT INTO 
                wallaby.run(name, sanity_thresholds) 
            VALUES 
                ('%s', '{"flux": 10, "spatial_extent": [10, 10], "spectral_extent": [10, 10], "uncertainty_sigma": 5}')
            RETURNING id
            """ % (name)
        )

        # Write instance (arbitrary boundary)
        run_id = result[0]['id']
        boundary = {0, 1702, 2802, 4502, 0, 269}
        params = read_json("test_data/params.json")
        reliability_plot = await read_file("test_data/sofia_test_output_rel.eps")
        result = await self.dbr.fetchrow_master(
            """
            INSERT INTO 
                wallaby.instance(
                    run_id, run_date, filename, boundary, flag_log, reliability_plot,
                    log, parameters, version, return_code, stdout, stderr
                )
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING id
            """,
            run_id, datetime.datetime.now(), name, boundary, None, reliability_plot,
            None, params, None, None, None, None
        )

        # Write detection
        instance_id = result[0]['id']
        link = "https://wallaby.aussrc.org/wallaby/vo/dl/dlmeta?ID="
        content = await read_file("test_data/sofia_058_cat.xml", mode='r')
        cat = xmltodict.parse(content)
        tr = cat['VOTABLE']['RESOURCE']['TABLE']['DATA']['TABLEDATA']['TR']
        fields = []
        for _, j in enumerate(cat['VOTABLE']['RESOURCE']['TABLE']['FIELD']):
            fields.append(j['@name'])
        detection = {}
        for i, v in enumerate(tr[0]['TD']):
            try:
                detection[fields[i]] = float(v)
            except ValueError:
                detection[fields[i]] = v
        result = await self.dbr.fetchrow_master(
            """
            INSERT INTO 
                wallaby.detection(
                    run_id, instance_id, unresolved, name, x, y, z, x_min, x_max,
                    y_min, y_max, z_min, z_max, n_pix, f_min, f_max, f_sum, rel, flag, rms,
                    w20, w50, ell_maj, ell_min, ell_pa, ell3s_maj, ell3s_min, ell3s_pa, kin_pa,
                    err_x, err_y, err_z, err_f_sum, ra, dec, freq, l, b, v_rad, v_opt, v_app, access_url
                )
            VALUES
                (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                    $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, 
                    $37, $38, $39, $40, $41, $42
                )
            RETURNING id
            """,
            run_id, instance_id, True,
            detection['name'], detection['x'], detection['y'], detection['z'],
            detection['x_min'], detection['x_max'],
            detection['y_min'], detection['y_max'], detection['z_min'],
            detection['z_max'], detection['n_pix'], detection['f_min'],
            detection['f_max'], detection['f_sum'],
            detection['rel'], detection['flag'], detection['rms'],
            detection['w20'], detection['w50'], detection['ell_maj'],
            detection['ell_min'], detection['ell_pa'],
            detection['ell3s_maj'], detection['ell3s_min'],
            detection['ell3s_pa'], detection['kin_pa'], detection['err_x'],
            detection['err_y'], detection['err_z'], detection['err_f_sum'],
            detection['ra'], detection['dec'], detection['freq'], 
            None, None, None, None, None, link
        )
        detection_id = result[0]['id']

        # Wait
        time.sleep(5)

        # Check replica table for runs
        result_run = await self.dbr.query_replica(
            """
            SELECT * FROM wallaby.run WHERE name='%s'
            """ % (name)
        )
        for res in result_run:
            _, n_rows = res.split(" ")
            self.assertEqual(int(n_rows), 1)

        # Check instance table
        result_instance = await self.dbr.query_replica(
            """
            SELECT * FROM wallaby.instance WHERE filename='%s'
            """ % (name)
        )
        for res in result_instance:
            _, n_rows = res.split(" ")
            self.assertEqual(int(n_rows), 1)

        # Check detection table
        result_detection = await self.dbr.query_replica(
            """
            SELECT * FROM wallaby.detection WHERE id='%s'
            """ % (detection_id)
        )
        for res in result_instance:
            _, n_rows = res.split(" ")
            self.assertEqual(int(n_rows), 1)