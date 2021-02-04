"""
Author: Kamrul Hasan
Email: hasana.alive@gmail.com
Date: 20.12.2020
"""

from copy import deepcopy
import unittest
import json

import app
from init import create_app

# Uni testing the api with the single endpoints with some test data. The test can be defined in the configuration.json file.

_,config= create_app()
customer_id =config['unit_test_customerid']
expected_return = config['unit_test_expected_return']
BASE_URL = f'http://0.0.0.0:5005/spend/{customer_id}'


class TestFlaskApi(unittest.TestCase):

    def setUp(self):
        self.app = app.app.test_client()
        self.app.testing = True

    def test_get_one(self):
        response = self.app.get(BASE_URL)
        data = json.loads(response.get_data())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data, expected_return)

    def tearDown(self):
        # reset app.items to initial state
        return ''


if __name__ == "__main__":
    unittest.main()