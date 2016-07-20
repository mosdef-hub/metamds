import pymongo as pm

from pprint import pprint
from metamds.db import *

"""
DB_SCHEMA =
    {
     "test_db" :
        {
         "test_collection" :
            {
             "name" : str,
             "age" : int,
             "weight" : float,
             "height" : float 
            }
        }
    }
"""
   
doc_1 = {"name": "TJJ", "age": 21, "weight": 172.4}

additional_info = {"height" : 72.5}
query_info = {"weight" : [172.4], "age" : [21]}

final_doc = doc_1
final_doc["height"] = 72.5

host="127.0.0.1"
port=27017
db_name = "test_db"
coll_name = "test_collection_mds_8679305"

connection = pm.MongoClient(host, port, j=True)
database = connection[db_name]
collection = database[coll_name] 

def test_add_doc_db():
    add_doc_db(doc=doc_1, host=host, port=port, database=db_name, collection=coll_name)
    assert collection.find_one(doc_1) == doc_1

def test_update_doc():
    update_doc(existing_doc=doc_1, added_values=additional_info, host=host, port=port, 
               database=db_name, collection=coll_name)
    assert collection.find(additional_info).count() == 1

def test_query_sim():
    cursor = query_sim(host=host, port=port, database=db_name, collection=coll_name, **query_info)
    info = list()
    for doc in cursor:
        info = doc
    assert cursor.count() == 1 and info["name"] == "TJJ"

def test_retrieve_all():
    cursor = retrieve_all(host=host, port=port, database=db_name, collection=coll_name)
    pm_cursor = collection.find()
    assert cursor.count() == pm_cursor.count()

if __name__ == "__main__":
    test_add_doc_db()
    test_update_doc()
    test_query_sim()
    test_retrieve_all()
    collection.delete_one(final_doc)

