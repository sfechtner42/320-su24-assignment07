"""
Main driver for a simple social network project
revised for assignment 7
"""

import csv
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from multiprocessing import Process, cpu_count
import pymongo

import user_status

DATABASE = "databaseA07"


def get_mongo_client(connection_string="mongodb://localhost:27017/"):
    """
    Creates a MongoDB client instance
    """
    return pymongo.MongoClient(connection_string)


def init_user_collection(mongo_client, database_name=DATABASE, table_name="UserAccounts"):
    """
    Creates and returns a MongoDB collection for user data.
    """
    db = mongo_client[database_name]  # Access the specified database by name
    collection = db[table_name]
    return collection


def init_status_collection(mongo_client, database_name=DATABASE, table_name="StatusUpdates"):
    """
    Creates and returns a new instance of UserStatusCollection.
    """
    db = mongo_client[database_name]  # Access the specified database by name
    status_collection = user_status.UserStatusCollection(db[table_name])
    return status_collection


def load_users(filename, user_collection, batch_size=32):
    """
    Opens a CSV file with user data and adds it to an existing MongoDB collection
    """
    try:
        with open(filename, encoding="utf-8", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            user_data = []
            for row in reader:
                if all(key in row and row[key] for key in ["USER_ID", "EMAIL", "NAME", "LASTNAME"]):
                    user_data.append({
                        "_id": row["USER_ID"],  # Use USER_ID as the primary key
                        "user_email": row["EMAIL"],
                        "user_name": row["NAME"],
                        "user_last_name": row["LASTNAME"]
                    })

            # Process data in batches
            for i in range(0, len(user_data), batch_size):
                batch = user_data[i:i + batch_size]
                try:
                    user_collection.insert_many(batch)
                except pymongo.errors.DuplicateKeyError:
                    print('Mock duplicate key error')
                    return False

            return True
    except (FileNotFoundError, KeyError) as e:
        print(f"Error loading users: {e}")
        return False


def load_users_multiprocess(filename, host="localhost", port=27017, database_name=DATABASE, batch_size=1000):
    """
    Loads the user file with multiprocessing.
    """
    processors = cpu_count()

    # Read the file in chunks to minimize memory usage and avoid reading the entire file at once.
    data_chunks = pd.read_csv(filename, chunksize=batch_size)

    processes = []
    for chunk in data_chunks:
        process = Process(target=load_users_multiprocess_worker, args=(chunk, host, port, database_name,))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    return True


def load_users_multiprocess_worker(data, host, port, database_name):
    """
    Helper function for multiprocessing to load users.
    """
    client = pymongo.MongoClient(host, port)
    user_collection = init_user_collection(client, database_name)

    column_map = {"USER_ID": "_id", "EMAIL": "user_email", "NAME": "user_name", "LASTNAME": "user_last_name"}
    data.rename(columns=column_map, inplace=True)

    # Convert the data to a list of dictionaries for batch insertion
    user_records = data.to_dict("records")

    # Insert data in batches using insert_many
    with ThreadPoolExecutor() as executor:
        batch_size = len(user_records) // cpu_count() + 1
        batches = [user_records[i:i + batch_size] for i in range(0, len(user_records), batch_size)]
        for batch in batches:
            executor.submit(user_collection.insert_many, batch, ordered=False)

    client.close()

def load_status_updates(filename, status_collection, batch_size=100):
    """
    Loads status updates from a CSV file into the database in batches.
    """
    try:
        with open(filename, 'r', encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            status_updates = []

            for row in reader:
                status_updates.append({
                    "_id": row['STATUS_ID'],
                    "user_id": row['USER_ID'],
                    "status_text": row['STATUS_TEXT']
                })

            # Process data in batches
            for i in range(0, len(status_updates), batch_size):
                batch = status_updates[i:i + batch_size]
                if not status_collection.batch_load_statuses(batch):
                    print(f"Error loading batch of statuses starting at index {i}")
                    return False

            return True
    except FileNotFoundError:
        # logger.debug("File %s was not found", filename)
        return False


def concurrent_batch_load_statuses(self, data, batch_size=1000, max_workers=4):
    """
    Concurrently loads batches of statuses using ThreadPoolExecutor.
    Allows for testing with different batch sizes.
    """
    def load_batch(batch):
        try:
            self.database.insert_many(batch, ordered=False)
        except pymongo.errors.BulkWriteError as error:
            write_errors = error.details['writeErrors']
            for error in write_errors:
                if error['code'] != 11000:  # If not a DuplicateKeyError
                    print(f"Unexpected error in batch: {error}")
            return False
        return True

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            futures.append(executor.submit(load_batch, batch))

        results = [f.result() for f in futures]
    return all(results)

# def load_status_updates_multiprocess(filename, host="localhost", port=27017, database_name=DATABASE):
#     """
#     Loads the status updates file with multiprocessing.
#     """
#     file_length = len(pd.read_csv(filename))
#     processors = cpu_count()
#     optimal_batch_size = int(file_length / processors) + 1
#     chunks = pd.read_csv(filename, chunksize=optimal_batch_size, iterator=True)
#
#     processes = []
#     for _ in range(processors):
#         try:
#             data = next(chunks)
#         except StopIteration:
#             break
#         # maybe comment out below this like in class?
#         process = Process(target=load_user_status_multiprocess_worker, args=(data, host, port, database_name,))
#         process.start()
#         processes.append(process)
#
#     for process in processes:
#         process.join()
#
#     return True
#
#
# def load_user_status_multiprocess_worker(data, host, port, database_name):
#     """
#     Helper function for multiprocessing to load status updates.
#     """
#     client = pymongo.MongoClient(host, port)
#     user_collection = init_user_collection(client, database_name)
#     status_collection = init_status_collection(client, database_name)
#     column_map = {"STATUS_ID": "_id", "USER_ID": "user_id", "STATUS_TEXT": "status_text"}
#     data.rename(columns=column_map, inplace=True)
#
#     ids = data["user_id"].unique().tolist()
#     matched_users = user_collection.count_documents({"_id": {"$in": ids}})
#
#     if matched_users != len(ids):
#         print("Statuses did not have corresponding User IDs.")
#         return False
#
#     status_collection.batch_load_statuses(data.to_dict("records"))
#     return True


def add_user(user_id, email, user_name, user_last_name, user_collection):
    """
    Creates a new instance of Users and stores it in user_collection
    """
    user = {
        "_id": user_id,
        "user_email": email,
        "user_name": user_name,
        "user_last_name": user_last_name
    }
    try:
        user_collection.insert_one(user)
        return True
    except pymongo.errors.DuplicateKeyError:
        return False


def update_user(user_id, email, user_name, user_last_name, user_collection):
    """
    Updates the values of an existing user
    """
    query = {"_id": user_id}
    new_values = {"$set": {
        "user_email": email,
        "user_name": user_name,
        "user_last_name": user_last_name
    }}
    result = user_collection.update_one(query, new_values)
    return result.modified_count > 0


def delete_user(user_id, user_collection, status_collection):
    """
    Deletes a user from user_collection and associated statuses from status_collection.
    """
    # First, attempt to delete the user
    user_result = user_collection.delete_one({"_id": user_id})

    if user_result.deleted_count > 0:
        # If the user was deleted, delete all associated statuses
        status_result = status_collection.delete_many({"user_id": user_id})
        print(f"Deleted {status_result.deleted_count} statuses associated with UserID {user_id}")
        # logger.debug("User ID %s successfully deleted from the database", user_id)
        return True

    return False


def search_user(user_id, user_collection):
    """
    Searches for a user in user_collection(which is an instance of UserCollection).
    """
    return user_collection.find_one({"_id": user_id})


def add_status(user_id, status_id, status_text, status_collection, user_collection):
    """
    Creates a new instance of UserStatus and stores it in status_collection
    """
    user_exists = user_collection.find_one({"_id": user_id})

    if not user_exists:
        return False  # User does not exist, status cannot be added

    return status_collection.add_status(status_id, user_id, status_text)


def update_status(status_id, user_id, status_text, status_collection):
    """
    Updates the values of an existing status_id
    """
    return status_collection.modify_status(status_id, user_id, status_text)


def delete_status(status_id, status_collection):
    """
    Deletes a status_id from status_collection.
    """
    return status_collection.delete_status(status_id)


def search_status(status_id, status_collection):
    """
    Searches for a status in status_collection
    """
    return status_collection.search_status(status_id)
