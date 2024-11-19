"""
Provides a basic frontend
From A03, converted from peewee to mongodb
"""

import sys
import main

DATABASE = "databaseA07"
# pylint: disable = E0606

def load_users():
    """
    Loads user accounts from a file
    """
    filename = input("Enter filename of user file: ")
    if not main.load_users(filename, user_collection):
        print("An error occurred while loading users.")
    else:
        print("Accounts loaded successfully.")


def load_status_updates():
    """
    Loads status updates from a file
    """
    filename = input("Enter filename for status file: ")
    if not main.load_status_updates(filename, status_collection):
        print("An error occurred while loading status updates.")
    else:
        print("Status updates loaded successfully.")


def add_user():
    """
    Adds a new user into the database
    """
    user_id = input("User ID: ")
    email = input("User email: ")
    user_name = input("User name: ")
    user_last_name = input("User last name: ")
    if not main.add_user(user_id, email, user_name, user_last_name, user_collection):
        print("This user already exists!")
    else:
        print("User was successfully added")


def update_user():
    """
    Updates information for an existing user
    """
    user_id = input("User ID: ")
    email = input("User email: ")
    user_name = input("User name: ")
    user_last_name = input("User last name: ")
    if not main.update_user(user_id, email, user_name, user_last_name, user_collection):
        print("An error occurred while trying to update user, check user id")
    else:
        print("User was successfully updated")


def search_user():
    """
    Searches a user in the database
    """
    user_id = input("Enter user ID to search: ")
    result = main.search_user(user_id, user_collection)
    if result is None:
        print("ERROR: User does not exist")
    else:
        print(f"User ID: {result['_id']}")
        print(f"Email: {result['user_email']}")
        print(f"Name: {result['user_name']}")
        print(f"Last name: {result['user_last_name']}")


def delete_user():
    """
    Deletes user from the database and associated statuses.
    """
    user_id = input("User ID: ")
    if not main.delete_user(user_id, user_collection, status_collection):
        print("User does not exist!")
    else:
        print("User and associated statuses were successfully deleted")


def add_status():
    """
    Adds a new status into the database
    """
    user_id = input("User ID: ")
    status_id = input("Status ID: ")
    status_text = input("Status text: ")
    if not main.add_status(user_id, status_id, status_text, status_collection, user_collection):
        print("An error occurred while trying to add new status; check if the user exists")
    else:
        print("New status was successfully added")


def update_status():
    """
    Updates information for an existing status
    """
    status_id = input("Status ID: ")
    user_id = input("User ID: ")
    status_text = input("Status text: ")
    if not main.update_status(status_id, user_id, status_text, status_collection):
        print("An error occurred while trying to update status; check status and user ids")
    else:
        print("Status was successfully updated")


def search_status():
    """
    Searches a status in the database
    """
    status_id = input("Enter status ID to search: ")
    result = main.search_status(status_id, status_collection)
    if result is False:
        print("ERROR: Status does not exist")
    else:
        print(f"Status ID: {result['_id']}")
        print(f"User ID: {result['user_id']}")
        print(f"Status text: {result['status_text']}")


def delete_status():
    """
    Deletes status from the database
    """
    status_id = input("Status ID: ")
    if not main.delete_status(status_id, status_collection):
        print("that status id does not exist")
    else:
        print("Status was successfully deleted")


def quit_program():
    """
    Quits program
    """
    sys.exit()


if __name__ == "__main__":
    mongo_client = main.get_mongo_client()
    user_collection = main.init_user_collection(mongo_client, database_name=DATABASE)
    status_collection = main.init_status_collection(mongo_client, database_name=DATABASE)
    menu_options = {
        "A": load_users,
        "B": load_status_updates,
        "C": add_user,
        "D": update_user,
        "E": search_user,
        "F": delete_user,
        "G": add_status,
        "H": update_status,
        "I": search_status,
        "J": delete_status,
        "Q": quit_program,
    }
    while True:
        user_selection = input(
            """
                            A: Load user database
                            B: Load status database
                            C: Add user
                            D: Update user
                            E: Search user
                            F: Delete user
                            G: Add status
                            H: Update status
                            I: Search status
                            J: Delete status
                            Q: Quit

                            Please enter your choice: """
        ).upper()
        if user_selection.upper() in menu_options:
            menu_options[user_selection]()
        else:
            print("Invalid option")
