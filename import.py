import os
import subprocess
import sys
import time

import pymongo
from pymongo import MongoClient, UpdateOne, WriteConcern
from pymongo.errors import BulkWriteError
from config import parameters as conf
from data import Data


connection = MongoClient(host=conf["host"],
                         port=conf["port"],
                         w=conf["write_concern"],
                         readConcernLevel=conf["read_concern"])
db = connection[conf["database"]]

# Schema
extract_orderline = conf['extract_orderline']

# Mongoimport path
mongoimport = conf["mongoimport"]


def debug(message):
        if conf['debug']:
            print(message)


def verify_mongoimport():
    "Verify the path of the mongoimport"
    global mongoimport
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    # Get path of all mongoimport
    mongoimport_paths = subprocess.Popen(["whereis", "mongoimport"],
                                         stdout=subprocess.PIPE)
    list_of_import_path = mongoimport_paths.stdout.readline().split()
    list_of_import_path = [mongo for mongo in list_of_import_path
                                           if os.path.isfile(mongo)]
    # Question user
    if not mongoimport:
        for path in list_of_import_path:
            prompt = "Is `{}` your mongoimport's path? [y/n] > ".format(path)
            cont = str(raw_input(prompt))
            while cont.lower() not in (valid.keys()):
                cont = str(raw_input("Please enter only [y/n] > "))
            if valid[cont.lower()]:
                mongoimport = path
                return
        print("It seems that none of the path are valid cqlsh path...")
        print("Program will now terminate...")
        sys.exit()


def upload_data():
    csv_generator = Data()
    list_of_processed_files = csv_generator.preprocess()
    mongoimport_start = time.time()
    for collection, filepath in list_of_processed_files:
        # Import processed csv file
        if conf['debug']:
            subprocess.call([mongoimport,
                             "--db", conf["database"],
                             "--collection", collection,
                             "--writeConcern", conf["write_concern"],
                             "--drop",
                             "--host", conf["host"],
                             "--port", str(conf["port"]),
                             "--ignoreBlanks",
                             "--numInsertionWorkers", conf["insert_workers"],
                             "--type", "csv",
                             "--headerline",
                             "--file", filepath])
        else:
            # Silent
            subprocess.call([mongoimport,
                             "--db", conf["database"],
                             "--collection", collection,
                             "--writeConcern", conf["write_concern"],
                             "--drop",
                             "--host", conf["host"],
                             "--port", str(conf["port"]),
                             "--ignoreBlanks",
                             "--numInsertionWorkers", conf["insert_workers"],
                             "--type", "csv",
                             "--headerline",
                             "--file", filepath],
                            stdout=open(os.devnull, "wb"))
    mongoimport_end = time.time()
    debug("Time taken for mongoimport: {}s\n"
          .format(mongoimport_end - mongoimport_start))


def create_indexes():
    index_start = time.time()
    # Index: Warehouse
    db.warehouse.create_index([("w_id", pymongo.ASCENDING)], unique=True)
    # Index:District
    db.district.create_index([("w_id", pymongo.ASCENDING),
                              ("d_id", pymongo.ASCENDING)], unique=True)
    # Index: Orders
    db.orders.create_index([("w_id", pymongo.ASCENDING),
                            ("d_id", pymongo.ASCENDING),
                            ("o_id", pymongo.ASCENDING)], unique=True)
    # Index: Stock
    db.stock.create_index([("w_id", pymongo.ASCENDING),
                           ("i_id", pymongo.ASCENDING)], unique=True)
    # Index: Customer
    db.customer.create_index([("w_id", pymongo.ASCENDING),
                              ("d_id", pymongo.ASCENDING),
                              ("c_id", pymongo.ASCENDING)], unique=True)
    # Index: c_balance
    db.customer.create_index([("c_balance", pymongo.DESCENDING)])
    # Index: Orderline
    if extract_orderline:
        db.orderline.create_index([("w_id", pymongo.ASCENDING),
                                   ("d_id", pymongo.ASCENDING),
                                   ("o_id", pymongo.ASCENDING),
                                   ("ol_number", pymongo.ASCENDING)],
                                  unique=True)
    index_end = time.time()
    debug("Index creation: {}s\n".format(index_end - index_start))

def shard():
    index_start = time.time()
    connection.admin.command('enableSharding', conf["database"])
    connection.admin.command('shardCollection', conf["database"]+'.warehouse',
                             key={'w_id': 1})
    connection.admin.command('shardCollection', conf["database"]+'.district',
                             key={'w_id': 1})
    connection.admin.command('shardCollection', conf["database"]+'.orders',
                             key={'w_id': 1})
    connection.admin.command('shardCollection', conf["database"]+'.stock',
                             key={'w_id': 1})
    connection.admin.command('shardCollection', conf["database"]+'.customer',
                             key={'w_id': 1})
    from collections import OrderedDict
    if extract_orderline:
        connection.admin.command('shardCollection',
                                 conf["database"]+'.orderline',
                                 key=OrderedDict([('w_id', 1), ('d_id', 1)])
                                 )
    index_end = time.time()
    debug("SHarding: {}s\n".format(index_end - index_start))

def preprocess_data():
    # Helper function
    def convert_str_to_list(string, is_int=False):
        array = string.strip("[]").replace("'", "").split(",")
        if is_int:
            array = [int(val.strip()) for val in array]
        else:
            array = [(string.strip()) for string in array]
        return array

    def get_order_updates(order):
        index = {"w_id": int(order.w_id), "d_id": int(order.d_id),
                 "o_id": int(order.o_id)}
        if not extract_orderline:
            update_elements = {"orderline": order.orderline_set,
                               "popular_items": order.popular_items,
                               "popular_items_name": order.popular_items_name,
                               "ordered_items": order.ordered_items}
        else:
            update_elements = {"popular_items": order.popular_items,
                               "popular_items_name": order.popular_items_name,
                               "ordered_items": order.ordered_items}
        update = UpdateOne(index, {"$set": update_elements})
        return update
    # Process orders; convert string into array
    debug("Begin preprocessing of orders collection\n")
    csv_generator = Data()
    order_update_request = []
    # Create dataframe for fast processing
    proc_dataframe_start = time.time()
    proc_orders = csv_generator.read_processed_csv("mongo_orders")
    orderlines = csv_generator.read_processed_csv("mongo_orderline", False)
    # Convert all to string type, as proc_orders's field is str type
    for id in ["w_id", "d_id", "o_id"]:
        orderlines[id] = orderlines[id].astype('str')
    # Get the list of documents for each order's orderline
    # Ignore if extract_orderline=True
    if not extract_orderline:
        proc_orderlines = orderlines.groupby(["w_id", "d_id", "o_id"])\
                                    .apply(lambda x: x[["ol_number",
                                                        "ol_i_id",
                                                        "ol_i_name",
                                                        "ol_amount",
                                                        "ol_supply_w_id",
                                                        "ol_quantity",
                                                        "ol_dist_info"]].
                                           to_dict(orient='records'))\
                                    .reset_index()
        # Rename columns
        proc_orderlines.columns = ["w_id", "d_id", "o_id", "orderline_set"]
        proc_orders = proc_orders.merge(proc_orderlines,
                                        on=["w_id", "d_id", "o_id"],
                                        how='left')
    # Convert string type to list object for mongodb
    proc_orders["popular_items"] = proc_orders["popular_items"].\
                                            map(lambda x:
                                                convert_str_to_list(x, True))
    proc_orders["popular_items_name"] = proc_orders["popular_items_name"].\
                                            map(lambda x:
                                                convert_str_to_list(x, False))
    proc_orders["ordered_items"] = proc_orders["ordered_items"].\
                                            map(lambda x:
                                                convert_str_to_list(x, True))
    # Ignore if extract_orderline=True
    if not extract_orderline:
        proc_orders = proc_orders[["w_id", "d_id", "o_id", "orderline_set",
                                   "popular_items", "popular_items_name",
                                   "ordered_items"]]
    else:
        proc_orders = proc_orders[["w_id", "d_id", "o_id", "popular_items",
                                   "popular_items_name", "ordered_items"]]
    proc_orders["update"] = proc_orders.apply(func=get_order_updates, axis=1)
    proc_dataframe_end = time.time()
    debug("Processing of main order dataframe: {}s\n"
          .format(proc_dataframe_end - proc_dataframe_start))
    order_update_request = list(proc_orders["update"].values)
    bulk_write_start = time.time()
    try:
        # Access "orders" collection, with specific write concern
        # Check if write concern is a string or not
        write_concern = conf["write_concern"]
        write_concern = int(write_concern) if write_concern.isdigit()\
                                            else write_concern
        write = WriteConcern(w=write_concern)
        orders_collection = db.get_collection('orders',
                                              write_concern=write)
        result = orders_collection.bulk_write(order_update_request,
                                              ordered=False)
        debug(result.bulk_api_result)
    except BulkWriteError as error:
        debug("Preprocessing of orders collection failed\n")
        debug(error.details)
    bulk_write_end = time.time()
    debug("Bulk write for Orders collection: {}s\n"
          .format(bulk_write_end - bulk_write_start))


def cleanup():
    connection.drop_database(conf["database"])
    debug("Dropped database {}\n".format(conf["database"]))


def main():
    main_start = time.time()
    cleanup()
    verify_mongoimport()
    upload_data()
    create_indexes()
    shard()
    preprocess_data()
    main_end = time.time()
    debug("Total time taken: {}s\n".format(main_end - main_start))


main()

