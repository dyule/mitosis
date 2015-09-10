import json
from py2neo.core import Node
from version_history.connection import Connection
from version_history.history import History

__author__ = 'dyule'

import unittest


class TestHistory(unittest.TestCase):
    def test_something(self):
        pass

    def test_commit_add_operations(self):
        # Initialize the repository
        # Create some add operations
        # Commit them
        # Check if the database looks right
        operations = []

