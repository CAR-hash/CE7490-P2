import unittest
from controller.controller import *
import requests
import json

class TestNet(unittest.TestCase):
    def test_basic_flask(self):
        post_dict = {'key1':'value1','key2':4}
        print(requests.post("http://localhost:5000/initialize", json.dumps(post_dict)))
        pass


if __name__ == '__main__':
    unittest.main()