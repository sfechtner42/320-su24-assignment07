'''
Functions to test multiprocessing performance
'''

import os
import time
import csv
from unittest.mock import patch
import main


# pylint: disable = C0301, W0621, C0103, W1514, W0718
def user_multiprocess_load():
    '''
    multiprocessing users load
    '''
    file = "accounts.csv"
    with patch("builtins.input", return_value=file):
        main.load_users_multiprocess(file)


def batch_load_statuses():
    '''
    load statuses from csv file in batches
    '''
    file = "status_updates.csv"
    client = main.get_mongo_client()
    status_collection = main.init_status_collection(client, "databaseA07")
    main.load_status_updates(file, status_collection)
    # status_collection.concurrent_batch_load_statuses(data, batch_size=batch_size)


def user_load(batch_size):
    '''
    load users from csv file in batches
    '''
    file = "accounts.csv"
    client = main.get_mongo_client()
    user_collection = main.init_user_collection(client, "databaseA07")
    main.load_users(file, user_collection, batch_size=batch_size)


def status_load(batch_size):
    '''
    load statuses from csv file in batches
    '''
    file = "status_updates.csv"
    client = main.get_mongo_client()
    main.init_user_collection(client, "databaseA07")
    status_collection = main.init_status_collection(client, "databaseA07")
    main.load_status_updates(file, status_collection, batch_size=batch_size)


if __name__ == "__main__":
    db_name = "databaseA07"
    iterations = 10
    batch_sizes = [50, 100, 250, 500, 1000, 5000, 10000, 20000, 50000]  # List of batch sizes to test

    # Initialize CSV file with a header if it doesn't exist
    csv_file = "fulltestresultsrecheck.csv"
    file_exists = os.path.isfile(csv_file)

    if not file_exists:
        with open(csv_file, "w", newline="") as csvfile:
            field_names = ["batch_size", "function", "iteration", "time"]
            writer = csv.DictWriter(csvfile, fieldnames=field_names, delimiter=",")
            writer.writeheader()

    test_functions = {
        "mp": {"user": user_multiprocess_load, "status": batch_load_statuses},
        "regular": {"user": user_load, "status": status_load},
    }

    for batch_size in batch_sizes:
        for iteration in range(iterations):
            for key, functions in test_functions.items():
                user_fun = functions["user"]
                status_fun = functions["status"]

                # Test the user function
                start_time = time.time()
                if key == "mp":
                    user_fun()
                else:
                    user_fun(batch_size)
                end_time = time.time()
                delta_user = end_time - start_time

                # Test the status function
                start_time = time.time()
                if key == "mp":
                    status_fun()
                else:
                    status_fun(batch_size)
                end_time = time.time()
                delta_status = end_time - start_time

                # Clear the database for the next test iteration
                client = main.get_mongo_client()
                client.drop_database(db_name)

                # Write results to CSV file
                try:
                    with open(csv_file, "a", newline="") as csvfile:
                        field_names = ["batch_size", "function", "iteration", "time"]
                        writer = csv.DictWriter(csvfile, fieldnames=field_names, delimiter=",")
                        writer.writerow({"batch_size": batch_size, "function": f"{key}_user", "iteration": iteration,
                                         "time": delta_user})
                        writer.writerow({"batch_size": batch_size, "function": f"{key}_status", "iteration": iteration,
                                         "time": delta_status})
                except Exception as e:
                    print(f"Error writing to CSV file: {e}")
