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
        ret = run.run_flink_task(filename)
        print(type(ret))
        print(ret)
        self.assertEqual(0, ret.returncode)


        filename = ""
        ret = run.run_flink_task(filename)
        print(type(ret))
        print(ret)
        self.assertNotEqual(0, ret.returncode)

        filename = "ABC.jar"
        ret = run.run_flink_task(filename)
        print(type(ret))
        print(ret)
        self.assertNotEqual(0, ret.returncode)
        

if __name__ == '__main__':
    unittest.main()