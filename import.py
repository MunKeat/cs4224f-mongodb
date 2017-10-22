from config import parameters as conf
from data import Data

import os

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

import subprocess

connection = MongoClient()
db = connection[conf["database"]]


# Todo: Remove hardcoding of mongoimport
mongoimport = "/home/stuproj/cs4224f/mk-mongo/mongodb-linux-x86_64-3.4.7/bin/mongoimport"


def debug(message):
        if conf['debug']:
            print(message)


def create_database():
    pass


def upload_data():
    csv_generator = Data()
    list_of_processed_files = csv_generator.preprocess()
    for collection, filepath in list_of_processed_files:
        # Import processed csv file
        if conf['debug']:
            subprocess.call([mongoimport,
                            "--db", conf["database"],
                            "--collection", collection,
                            "--type", "csv",
                            "--headerline",
                            "--file", filepath])
        else:
            # Silent
            subprocess.call([mongoimport,
                            "--db", conf["database"],
                            "--collection", collection,
                            "--type", "csv",
                            "--headerline",
                            "--file", filepath], stdout=open(os.devnull, 'wb'))

def preprocess_data():
    def convert_str_to_list(string):
        # Remove bracket
        array = string.strip("[]").split(",")
        array = [element.replace("'", "").strip() for element in array]
        return array
    # Process orders; convert string into array
    debug("Begin preprocessing of orders collection\n")
    order_update_request = []
    for order in db.orders.find():
        _id = order['_id']
        popular_item_id = convert_str_to_list(order['popular_item_id'])
        popular_item_name = convert_str_to_list(order['popular_item_name'])
        ordered_items = convert_str_to_list(order['ordered_items'])
        # Convert to proper type
        popular_item_id = [int(id) for id in popular_item_id]
        popular_item_name = [name for name in popular_item_name]
        ordered_items = [int(id) for id in ordered_items]
        # To Update
        update_elements = {"popular_item_id": popular_item_id,
                           "popular_item_name": popular_item_name,
                           "ordered_items": ordered_items}
        # Update
        update = UpdateOne({"_id": _id}, {"$set": update_elements})
        order_update_request.append(update)
    try:
        db.orders.bulk_write(order_update_request)
    except BulkWriteError as error:
        debug("Preprocessing of orders failed\n")
        print(error.details)
    debug("End preprocessing of orders collection\n")


def cleanup():
    connection.drop_database(conf["database"])


def main():
    cleanup()
    upload_data()
    preprocess_data()


main()
