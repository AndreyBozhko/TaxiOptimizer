import unittest
from mock import mock_open, patch

import sys
sys.path.append("../helpers/")

import os
import json

import helpers



class TestHelpersMethods(unittest.TestCase):
    """
    class that contains unit tests for methods from helpers.py
    """

    def setUp(self):
        """
        initialization of test suites
        """
        pass



    def tearDown(self):
        """
        cleanup after test suites
        """
        pass



    def test_determine_time_slot(self):
        # test on valid input
        time = "2008-05-13 15:26:35"
        ans  = 15*6+2
        self.assertEqual(ans,
                helpers.determine_time_slot(time),
                "time_slot is incorrect")
        # test on invalid input, Exception expected
        time = "2008-05-13 15:62:35"
        self.assertRaises(ValueError,
                lambda: helpers.determine_time_slot(time))



    def test_determine_block_ids(self):
        # test if correctly calculates block ids
        lon, lat = -60.0008, 60.0008
        ids = [(2849, 3900), (56996, 78003)]
        self.assertItemsEqual(ids,
                helpers.determine_block_ids(lon, lat),
                'incorrect block ids returned')



    def test_get_neighboring_blocks(self):
        # test on any given block
        block = (4, 5)
        neighbors = [(4, 6), (4, 4), (3, 6), (3, 5),
                     (3, 4), (5, 6), (5, 5), (5, 4)]
        self.assertItemsEqual(neighbors,
                helpers.get_neighboring_blocks(block),
                "incorrect neighbors returned")



    def test_determine_subblock_lonlat(self):
        # test if correctly finds coordinates
        subblock = (83745, 2304)
        lonlat = [-53.313625, 41.076125]
        self.assertItemsEqual(lonlat,
                helpers.determine_subblock_lonlat(subblock),
                'incorrect coordinates returned')



    def test_map_schema(self):
        schema = {"DELIMITER": ";", "FIELDS": {"field1": {"index": 3, "type": "int"},
                                               "field2": {"index": 1, "type": "str"}}}
        line = ";".join(["a","b","c","4","e"])
        ans = {"field1": 4, "field2": "b"}
        # test if schema parses
        self.assertEqual(ans,
                helpers.map_schema(line, schema),
                "incorrect schema produced")
        # test if parsing fails because of missing fields
        self.assertIsNone(
                helpers.map_schema(line[:3], schema),
                "did not return None when fields are missing")



    def test_add_block_fields(self):
        # test if correctly adds all the block fields
        dic = {"latitude": 60.0008, "longitude": -60.0008}
        dic2 = dict(dic)
        dic2["block_id"], dic2["sub_block_id"] = (2849, 3900), (56996, 78003)
        dic2["block_latid"], dic2["block_lonid"] = 2849, 3900
        self.assertEqual(dic2,
                helpers.add_block_fields(dic),
                'did not insert correct block ids')
        # test if returns None if unable to add
        del dic["longitude"]
        self.assertIsNone(helpers.add_block_fields(dic),
                'did not return None when fields are missing')



    def test_add_time_slot_field(self):
        # test if correctly adds time_slot
        dic = {"datetime": "2020-07-23 06:23:15"}
        dic2 = dict(dic)
        dic2["time_slot"] = 38
        self.assertEqual(dic2,
                helpers.add_time_slot_field(dic),
                'did not insert correct time_slot')
        # test if returns None if unable to add
        del dic2["datetime"]
        self.assertIsNone(helpers.add_time_slot_field(dic2),
                'did not return None')



    def test_check_passengers(self):
        # test on valid input
        d1 = {"passengers": 3}
        self.assertEqual(3,
                helpers.check_passengers(d1)["passengers"],
                "incorrect number of passengers returned")
        # test for < 1 passengers, non-int passengers and absence of field passenger
        # None expected
        d2 = {"passengers": 0}
        d3 = {"somefield": "5"}
        for d in [d2, d3]:
            self.assertIsNone(
                    helpers.check_passengers(d),
                    "did not return None for input {}".format(d))



    def test_parse_config(self):
        # test if correctly parses the config file
        conf = {"field1": "val1",
                "field2": {"subfield1": 2, "subfield2": "3"}}

        with patch("__builtin__.open",
                   mock_open(read_data=json.dumps(conf))) as mock_file:

            self.assertEqual(conf,
                    helpers.parse_config(mock_file),
                    "fail to properly read config from file")



    def test_replace_envvars_with_vals(self):
        # test if correctly parses environmental variables
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
