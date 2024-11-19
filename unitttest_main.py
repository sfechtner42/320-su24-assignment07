"""
Unittests for main.py
"""

import unittest
from unittest.mock import patch, MagicMock, mock_open, call

import pandas as pd
import pymongo
import main


# pylint: disable = C0301

class TestMainUserFunctions(unittest.TestCase):
    """
    Unit tests for main.py user-related functions.
    """

    def setUp(self):
        """
        Setup for testing.
        """
        self.mock_user_collection = MagicMock()

    @patch('main.pymongo.MongoClient')
    def test_get_mongo_client(self, mock_get_mongo_client):
        """
        set up mongo client for testing
        """
        main.get_mongo_client()
        mock_get_mongo_client.assert_called_once()

    def test_init_user_collection(self):
        """
        Test for main's init_user_collection function
        """
        # setup mock DB
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        # Set up the return values for the mock objects
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection

        # Call the function being tested
        collection = main.init_user_collection(mock_client)

        # Assert that the function returns the expected collection
        self.assertEqual(collection, mock_collection)
        mock_client.__getitem__.assert_called_once_with("databaseA07")
        mock_db.__getitem__.assert_called_once_with("UserAccounts")
    @patch('main.open')
    def test_load_users_success(self, _mock_open):
        """
        Tests successful loading of users from a CSV file.
        """
        mock_file = MagicMock()
        mock_user_collection = MagicMock()

        # Simulate file content for DictReader
        mock_file.__enter__.return_value = mock_file
        mock_file.read.return_value = "USER_ID,EMAIL,NAME,LASTNAME\n1,test@example.com,test,test"

        # Create a DictReader mock
        mock_reader = iter([
            {"USER_ID": "1", "EMAIL": "test@example.com", "NAME": "test", "LASTNAME": "test"}
        ])
        mock_file.__iter__.return_value = mock_reader

        with patch('csv.DictReader', return_value=mock_reader):
            result = main.load_users("users.csv", mock_user_collection)

        _mock_open.assert_called_once_with("users.csv", encoding="utf-8", newline="")
        mock_user_collection.insert_many.assert_called_once_with([{
            "_id": "1",
            "user_email": "test@example.com",
            "user_name": "test",
            "user_last_name": "test"
        }])
        self.assertTrue(result)

    def test_load_users_duplicate_key_error(self):
        """
        Test that load_users returns False when a DuplicateKeyError is raised.
        """
        mock_dictreader = MagicMock()
        mock_dictreader.__iter__.return_value = [
            {"USER_ID": "test1", "EMAIL": "test1@example.com", "NAME": "test1", "LASTNAME": "test1"},
            {"USER_ID": "test2", "EMAIL": "test2@example.com", "NAME": "test2", "LASTNAME": "test2"},
        ]

        mock_error = pymongo.errors.DuplicateKeyError("Mock duplicate key error")

        with patch("builtins.open", unittest.mock.mock_open()), patch(
                "csv.DictReader", return_value=mock_dictreader
        ), patch("builtins.print") as mock_print:
            # Mock insert_many to raise the DuplicateKeyError
            self.mock_user_collection.insert_many.side_effect = mock_error

            result = main.load_users("test_users.csv", self.mock_user_collection)
            self.mock_user_collection.insert_many.assert_called_once()
            self.assertFalse(result)

            mock_print.assert_called_with(
                "Mock duplicate key error"
            )

    def test_load_users_file_not_found(self):
        """
        Test error loading users into database from a CSV file.
        """
        with patch("builtins.open", side_effect=FileNotFoundError):
            result = main.load_users("nonexistent.csv", self.mock_user_collection)
            self.assertFalse(result)

    def test_add_user(self):
        """
        Test adding a user to the database.
        """
        self.mock_user_collection.insert_one.return_value = None
        result = main.add_user("SC", "sesame@uw.edu", "Sesame", "Chan", self.mock_user_collection)
        self.assertTrue(result)
        self.mock_user_collection.insert_one.assert_called_once()

    def test_add_user_duplicate_key_error(self):
        """
        Test that add_user returns False when a DuplicateKeyError is raised.
        """
        self.mock_user_collection.insert_one.side_effect = pymongo.errors.DuplicateKeyError("Duplicate key error")

        result = main.add_user(
            user_id="SC",
            email="sesame@wu.edu",
            user_name="Sesame",
            user_last_name="Chan",
            user_collection=self.mock_user_collection
        )

        expected_user = {
            "_id": "SC",
            "user_email": "sesame@wu.edu",
            "user_name": "Sesame",
            "user_last_name": "Chan"
        }
        self.mock_user_collection.insert_one.assert_called_with(expected_user)

        self.assertFalse(result)

    def test_update_user(self):
        """
        Test updating a user in the database.
        """
        self.mock_user_collection.update_one.return_value.modified_count = 1
        result = main.update_user(
            "SC", "newemail@uw.edu", "Sesame", "Chan", self.mock_user_collection
        )
        self.assertTrue(result)
        self.mock_user_collection.update_one.assert_called_once()

    def test_delete_user_user_not_found(self):
        """
        Test attempting to delete a user that does not exist, which should return False.
        """
        mock_status_collection = MagicMock()

        # Simulate deleting a user that does not exist
        self.mock_user_collection.delete_one.return_value.deleted_count = 0

        # Call the delete_user function with both collections
        result = main.delete_user("SC", self.mock_user_collection, mock_status_collection)

        self.assertFalse(result)
        self.mock_user_collection.delete_one.assert_called_once_with({"_id": "SC"})
        mock_status_collection.delete_many.assert_not_called()  # Statuses should not be deleted if the user doesn't exist

    def test_delete_user_user_found(self):
        """
        Test successfully deleting a user and associated statuses from the database.
        """
        mock_status_collection = MagicMock()

        # Simulate the user being found and deleted
        self.mock_user_collection.delete_one.return_value.deleted_count = 1

        # Simulate deleting associated statuses
        mock_status_collection.delete_many.return_value.deleted_count = 3

        # Call the delete_user function with both collections
        result = main.delete_user("SC", self.mock_user_collection, mock_status_collection)

        self.assertTrue(result)
        self.mock_user_collection.delete_one.assert_called_once_with({"_id": "SC"})
        mock_status_collection.delete_many.assert_called_once_with({"user_id": "SC"})

    def test_search_user(self):
        """
        Test searching for a user in the database.
        """
        self.mock_user_collection.find_one.return_value = {"_id": "SC", "user_email": "sesame@uw.edu"}
        result = main.search_user("SC", self.mock_user_collection)
        self.assertEqual(result["_id"], "SC")


class TestMainStatusFunctions(unittest.TestCase):
    """
    Unit tests for main.py status-related functions.
    """

    def setUp(self):
        """
        Setup for testing.
        """
        self.mock_status_collection = MagicMock()

    def test_init_status_collection(self):
        """
        Test for main's init_status_collection function
        """
        # Patch the UserStatusCollection in the user_status module
        with patch("user_status.UserStatusCollection") as mock_collection:
            # Create a mock MongoDB client
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_table = MagicMock()

            # Set up the mock client to return mock objects
            mock_client.__getitem__.return_value = mock_db
            mock_db.__getitem__.return_value = mock_table

            # Call the function being tested
            collection = main.init_status_collection(mock_client)

            # Assert that UserStatusCollection was called with the correct arguments
            mock_collection.assert_called_once_with(mock_table)
            self.assertEqual(collection, mock_collection.return_value)

    def test_load_status_updates_success(self):
        """
        Test loading status updates into the database from a CSV file.
        """
        mock_dictreader = MagicMock()
        mock_dictreader.__iter__.return_value = [
            {"STATUS_ID": "SC1", "USER_ID": "SC", "STATUS_TEXT": "Meow"},
            {"STATUS_ID": "SC2", "USER_ID": "SC", "STATUS_TEXT": "Food!"},
        ]

        with patch("builtins.open", unittest.mock.mock_open()), patch(
                "csv.DictReader", return_value=mock_dictreader
        ):
            result = main.load_status_updates("test_status.csv", self.mock_status_collection)

            # Ensure batch_load_statuses is called with the correct argument
            self.mock_status_collection.batch_load_statuses.assert_called_once_with([
                {"_id": "SC1", "user_id": "SC", "status_text": "Meow"},
                {"_id": "SC2", "user_id": "SC", "status_text": "Food!"}
            ])

            self.assertTrue(result)

    def test_load_status_updates_failure(self):
        """
        Test error loading status updates into database from a CSV file.
        """
        with patch("builtins.open", side_effect=FileNotFoundError):
            result = main.load_status_updates("nonexistent.csv", self.mock_status_collection)
            self.assertFalse(result)

    @patch('builtins.open', new_callable=mock_open,
           read_data="STATUS_ID,USER_ID,STATUS_TEXT\n1,user1,Hello\n2,user2,World")
    @patch('user_status.UserStatusCollection.batch_load_statuses')
    def test_load_status_updates_batch_error(self, mock_batch_load_statuses, _mock_open_instance):
        """
        Test load_status_updates returns False and handles batch load errors correctly.
        """
        mock_batch_load_statuses.return_value = False
        mock_status_collection = MagicMock()
        mock_status_collection.batch_load_statuses = mock_batch_load_statuses
        result = main.load_status_updates("test_status.csv", mock_status_collection)

        self.assertFalse(result)
        mock_batch_load_statuses.assert_called_once()
        self.assertEqual(mock_batch_load_statuses.call_count, 1)

    def test_add_status(self):
        """
        Test adding a status to the database.
        """
        mock_status_collection = MagicMock()
        mock_user_collection = MagicMock()

        mock_user_collection.find_one.return_value = {"_id": "SC"}

        mock_status_collection.add_status.return_value = True

        # Call the add_status function with both collections
        result = main.add_status("SC", "SC1", "Meow", mock_status_collection, mock_user_collection)

        self.assertTrue(result)
        mock_user_collection.find_one.assert_called_once_with({"_id": "SC"})
        mock_status_collection.add_status.assert_called_once_with("SC1", "SC", "Meow")

    def test_add_status_user_not_found(self):
        """
        Test attempting to add a status when the user does not exist, which should return False.
        """
        mock_status_collection = MagicMock()
        mock_user_collection = MagicMock()

        mock_user_collection.find_one.return_value = None

        result = main.add_status("SC", "SC1", "Meow", mock_status_collection, mock_user_collection)

        self.assertFalse(result)
        mock_user_collection.find_one.assert_called_once_with({"_id": "SC"})
        mock_status_collection.add_status.assert_not_called()  # Status should not be added if the user does not exist

    def test_update_status(self):
        """
        Test updating a status in the database.
        """
        self.mock_status_collection.modify_status.return_value = True
        result = main.update_status("SC1", "SC", "Updated Status!", self.mock_status_collection)
        self.assertTrue(result)
        self.mock_status_collection.modify_status.assert_called_once_with("SC1", "SC", "Updated Status!")

    def test_delete_status(self):
        """
        Test deleting a status from the database.
        """
        self.mock_status_collection.delete_status.return_value = True
        result = main.delete_status("status1", self.mock_status_collection)
        self.assertTrue(result)
        self.mock_status_collection.delete_status.assert_called_once_with("status1")

    def test_search_status(self):
        """
        Test searching for a status in the database.
        """
        self.mock_status_collection.search_status.return_value = {"_id": "status1", "user_id": "SC",
                                                                  "status_text": "Meow"}
        result = main.search_status("status1", self.mock_status_collection)
        self.assertEqual(result["_id"], "status1")

if __name__ == "__main__":
    unittest.main()
