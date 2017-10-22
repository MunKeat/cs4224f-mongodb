import os
import subprocess
import time

from pathos.multiprocessing import ProcessingPool as Pool
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from config import parameters as conf
from data import Data


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
    csv_generator = Data()
    order_update_request = []
    order_insert_request = []
    orderlines = csv_generator.read_original_csv("order-line")
    # Make parallel the following
    def convert_orders_attr_to_array(order):
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
        return update
    pool = Pool()
    pool.restart()
    start = time.time()
    order_update_request = pool.map(convert_orders_attr_to_array,
                                    db.orders.find())
    end = time.time()
    debug("Parallel processing of convert_orders_attr_to_array took {}s"\
          .format(end - start))
    try:
        db.orders.bulk_write(order_update_request)
    except BulkWriteError as error:
        debug("Preprocessing of orders collection failed\n")
        print(error.details)
    debug("Preprocessing of orders collection Part I complete\n")
    # Dataframe access is not included above;
    # pandas is not necessarily threadsafe
    for order in db.orders.find():
        _id = order['_id']
        w_id = order['w_id']
        d_id = order['d_id']
        o_id = order['o_id']
        # Insert list of documents
        orderline_set = orderlines[(orderlines["w_id"] == w_id) &
                                   (orderlines["d_id"] == d_id) &
                                   (orderlines["o_id"] == o_id)]
        orderline_set = orderline_set[["ol_number", "ol_i_id", "ol_amount",
                                       "ol_supply_w_id", "ol_quantity",
                                       "ol_dist_info"]]
        orderline_set = orderline_set.to_dict('records')
        insert_element = {"orderline": orderline_set}
        insert = InsertOne({"_id": _id}, {"$set": insert_element})
        order_insert_request.append(insert)
    try:
        db.orders.bulk_write(order_insert_request)
    except BulkWriteError as error:
        debug("Preprocessing of orders collection failed\n")
        print(error.details)
    debug("End preprocessing of orders collection\n")


def cleanup():
    connection.drop_database(conf["database"])


def main():
    cleanup()
    upload_data()
    preprocess_data()


main()
