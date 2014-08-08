"""Quick and easy way to get setup.py test to run py.test without any
custom setuptools/distutils code.

"""
import unittest
import pytest


class TestSuite(unittest.TestCase):
    def test_sqlalchemy(self):
        pytest.main(["-n", "4", "-q"])
