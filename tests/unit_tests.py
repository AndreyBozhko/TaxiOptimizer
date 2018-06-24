import unittest
from mock import mock_open, patch

import sys
sys.path.append("../helpers/")

import os
import errno
import json

import helpers



class TestHelpersMethods(unittest.TestCase):
    """
    class that contains unit tests for methods from helpers.py
    """

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_determine_time_slot(self):
        time = "2008-05-13 15:26:35"
        ans  = 15*6+2
        self.assertEqual(ans,
                helpers.determine_time_slot(time),
                "time_slot is incorrect")

        time = "2008-05-13 15:62:35"
        self.assertRaises(ValueError,
                lambda: helpers.determine_time_slot(time))


    def test_determine_block_ids(self):
        pass


    def test_get_neighboring_blocks(self):
        block = (4, 5)
        neighbors = [(4, 6), (4, 4), (3, 6), (3, 5),
                     (3, 4), (5, 6), (5, 5), (5, 4)]
        self.assertItemsEqual(neighbors,
                helpers.get_neighboring_blocks(block),
                "incorrect neighbors returned")


    def test_determine_subblock_lonlat(self):
        pass


    def test_map_schema(self):
        schema = {"DELIMITER": ";", "FIELDS": {"field1": 3, "field2": 1}}
        line = ";".join(["a","b","c","d","e"])
        ans = {"field1": "d", "field2": "b"}
        self.assertEqual(ans,
                helpers.map_schema(line, schema),
                "incorrect schema produced")
        self.assertIsNone(
                helpers.map_schema(line[:3], schema),
                "did not return None when fields are missing")


    def test_add_block_fields(self):
        pass


    def test_add_time_slot_field(self):
        pass


    def test_check_passengers(self):
        d1 = {"passengers": "3"}
        self.assertEqual(3,
                helpers.check_passengers(d1)["passengers"],
                "incorrect number of passengers returned")

        d2 = {"passengers": "0"}
        d3 = {"passengers": "qwerty"}
        d4 = {"somefield": "5"}
        for d in [d2, d3, d4]:
            self.assertIsNone(
                    helpers.check_passengers(d),
                    "did not return None for input {}".format(d))


    def test_parse_config(self):
        conf = {"field1": "val1",
                "field2": {"subfield1": 2, "subfield2": "3"}}

        with patch("__builtin__.open",
                   mock_open(read_data=json.dumps(conf))) as mock_file:

            self.assertEqual(conf,
                    helpers.parse_config(mock_file),
                    "fail to properly read config from file")


    def test_get_psql_config(self):
        pass


    def test_replace_envvars_with_vals(self):
        dic1 = {"field1": "$TESTENVVAR:15,$TESTENVVAR",
                "field2": {"sf1": 1, "sf2": "$TESTENVVAR"}}
        dic2 = {"field1": "test123:15,test123",
                "field2": {"sf1": 1, "sf2": "test123"}}

        with patch.dict(os.environ, {'TESTENVVAR': 'test123'}, clear=True):

            self.assertEqual(dic2,
                    helpers.replace_envvars_with_vals(dic1),
                    "did not correctly parse environmental variables")



if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestHelpersMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
