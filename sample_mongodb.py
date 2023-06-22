from pymongo import MongoClient
import datetime
import configparser
import certifi

# load the mongo credential values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
username = parser.get("mongo_config", "username")
password = parser.get("mongo_config", "password")
database_name = parser.get("mongo_config", "database_name")
collection = parser.get("mongo_config", "collection")


mongo_client = MongoClient(
    f"""mongodb+srv://{username}:{password}@{username.lower()}.y7gdbex.mongodb.net/?retryWrites=true&w=majority""",
    tlsCAFile = certifi.where()
    )

# connect to the db where the collection resides
mongo_db = mongo_client[database_name]

# choose the collection to query documents from
mongo_collection = mongo_db[collection]

event_1 = {
    "event_id": 1,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "signup"
    }

event_2 = {
    "event_id": 2,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "pageview"
    }

event_3 = {
    "event_id": 3,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "login"
    }


# insert the 3 documents
mongo_collection.insert_one(event_1)
mongo_collection.insert_one(event_2)
mongo_collection.insert_one(event_3)