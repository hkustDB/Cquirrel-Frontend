import unittest
import json
import os
import io

import sys 
sys.path.append(".") 
import run
from run import app


class TestUpload(unittest.TestCase):
    def setUp(self):
        self.app = app
        app.testing = True
        self.client = app.test_client()  

        # mock a json file
        self.json_content1 = {"swimming":"cool", "running":"hot"}
        self.json_str1 = json.dumps(self.json_content1, indent=4)
        self.json_file_path1 = "./json1.json"
        with open(self.json_file_path1, "w") as f:
            f.write(self.json_str1)

    def tearDown(self):
        if os.path.exists(self.json_file_path1):
            os.remove(self.json_file_path1)
        

    def test_upload(self):
        response = self.client.post(
            '/',
            data= {
                'json_file':(io.BytesIO(self.json_str1.encode()), "json1.json")
            },
            content_type='multipart/form-data',
            follow_redirects=True
        )
        # if the response include CodeGen, which means success
        self.assertIn(b"CodeGen", response.data)
        

    def tests_upload_empty(self):
        response = self.client.post(
            '/',
            data= {
                'json_file':(io.BytesIO(''.encode()), '')
            },
            content_type='multipart/form-data',
            follow_redirects=True
        )
        # if the response include CodeGen, which means success
        self.assertIn(b"json file is empty", response.data)

    def tests_upload_not_json(self):
        response = self.client.post(
            '/',
            data= {
                'json_file':(io.BytesIO('abc'.encode()), 'abc.txt')
            },
            content_type='multipart/form-data',
            follow_redirects=True
        )
        # if the response include CodeGen, which means success
        self.assertIn(b"uploaded file is not json file", response.data)


if __name__ == '__main__':
    unittest.main()