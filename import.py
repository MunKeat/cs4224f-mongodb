from config import parameters as conf
from data import Data

from pymongo import MongoClient

import subprocess

connection = MongoClient()
db = connection[conf["database"]]


# Todo: Remove hardcoding of mongoimport
mongoimport = "/home/stuproj/cs4224f/mk-mongo/mongodb-linux-x86_64-3.4.7/bin/mongoimport"


def create_database():
    pass


def upload_data():
    csv_generator = Data()
    list_of_processed_files = csv_generator.preprocess()
    for collection, filepath in list_of_processed_files:
        # Import processed csv file
        subprocess.call([mongoimport,
                        "--db", conf["database"],
                        "--collection", collection,
                        "--type", "csv",
                        "--headerline",
                        "--file", filepath])


def cleanup():
    connection.drop_database(conf["database"])


def main():
    cleanup()
    upload_data()


main()
