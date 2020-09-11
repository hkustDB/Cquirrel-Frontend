import unittest
import json
import os
import io

import sys 
sys.path.append("..") 
import run
from run import app

class TestRunFlink(unittest.TestCase):
    def setUp(self):
        self.app = app
        app.testing = True
        self.client = app.test_client()  


    def tearDown(self):
        return super().tearDown()


    def test_run_flink(self):
        filename = "WordCount.jar"
        output = run.run_flink_task(filename)
        self.assertIn("JobID", output)


        filename = ""
        output = run.run_flink_task(filename)
        self.assertIn("null", output)

        filename = "ABC.jar"
        output = run.run_flink_task(filename)
        self.assertIn("does not exist", output)
        

if __name__ == '__main__':
    unittest.main()