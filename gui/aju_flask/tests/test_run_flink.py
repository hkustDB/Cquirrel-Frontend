import unittest
import json
import os
import io

from aju_app import create_app
from aju_app import aju_utils


class TestRunFlink(unittest.TestCase):
    def setUp(self):
        self.app = create_app('testing')
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.client = self.app.test_client()

    def tearDown(self):
        self.app_context.pop()

    def test_run_flink(self):
        filename = "WordCount.jar"
        ret = aju_utils.run_flink_task(filename)
        self.assertEqual(0, ret.returncode)

        filename = ""
        ret = aju_utils.run_flink_task(filename)
        self.assertNotEqual(0, ret.returncode)

        filename = "ABC.jar"
        ret = aju_utils.run_flink_task(filename)
        self.assertNotEqual(0, ret.returncode)


if __name__ == '__main__':
    unittest.main()
