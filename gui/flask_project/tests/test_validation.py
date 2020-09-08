import unittest
import json
import os

import sys 
sys.path.append(".") 
import run
from run import app

class TestValidation(unittest.TestCase):
    def setUp(self):
        self.app = app
        app.testing = True
        self.client = app.test_client()

        # generate a positive json file
        self.positive_json_file_content = {'flink':1, 'spark':2, 'streaming':"yes"}
        self.positive_json_file_path = "./positive_json_file.json"
        with open(self.positive_json_file_path, 'w') as f:
            json.dump(self.positive_json_file_content, f, indent=4)

        # generate a negative json file
        self.negative_json_file_content = {'flink':1, 'spark':2, 'streaming':"yes"}
        self.negative_json_file_path = "./negative_json_file.json"
        with open(self.negative_json_file_path, 'w') as f:
            f.write(self.negative_json_file_path)


    def tearDown(self):
        # delete the positive json file
        if os.path.exists(self.positive_json_file_path):
            os.remove(self.positive_json_file_path)
        else:
            print("positive_json_file_path does not exist!")

        # delete the negative json file
        if os.path.exists(self.negative_json_file_path):
            os.remove(self.negative_json_file_path)
        else:
            print("negative_json_file_path does not exist!")


    def test_is_json_file_positive(self):        
        # test the function is_json_file()
        result = run.is_json_file(self.positive_json_file_path) 
        self.assertTrue(result)    

    
    def test_is_json_file_negative(self):
        result = run.is_json_file(self.negative_json_file_path)
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
