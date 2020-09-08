import unittest
import json
import os

import sys 
sys.path.append("..") 
import run
from run import app


class TestUpload(unittest.TestCase):
    def setUp(self):
        self.app = app
        app.testing = True
        self.client = app.test_client()  

    def tearDown(self):
        return super().tearDown()

    def test_upload(self):
        # mock a json file to upload
        
        return True



if __name__ == '__main__':
    unittest.main()