import unittest
import main
from main import app
import json
import os

class TestUpload(unittest.TestCase):
    def setUp(self):
        self.app = app
        app.testing = True
        self.client = app.test_client()

    def test_is_json_file(self):
        # generate a fake json file
        fake_json_file_content = {'flink':1, 'spark':2, 'streaming':"yes"}
        fake_json_file_path = "./fake_json_file.json"
        with open(fake_json_file_path, 'w') as f:
            json.dump(fake_json_file_content, f, indent=4)
        
        # test the function is_json_file()
        result = main.is_json_file(fake_json_file_path)
        
        # delete the fake json file
        if os.path.exists(fake_json_file_path):
            os.remove(fake_json_file_path)
        else:
            print("fake_json_file does not exist!")

        return result


if __name__ == '__main__':
    unittest.main()
