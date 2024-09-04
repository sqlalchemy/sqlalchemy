# stdlib
import os


BASE_DIR = os.path.split(os.path.dirname(__file__))[0]
TEST_DIR = os.path.split(BASE_DIR)[0]
SQLALCHEMY_DIR = os.path.split(TEST_DIR)[0]
