import pymongo as pm

# TODO: Add user//pw functionality for a hosted db and implement the get_uri function

def add_doc_db(doc, host="127.0.0.1", port=27017, database="shearing_simulations", 
               user=None, password=None, collection="tasks", update_duplicates=False):
    """adds a single document to a database

    Parameters
    ----------
    doc : dict
        Document being entered into a collection
    host : str, optional
        Database connection host (the default is 127.0.0.1, or the local computer being used)
    port : int, optional
        Database host port (default is 27017, which is the pymongo default port).
    database : str, optional
        Name of the database being used (default is shearing_simulations).
    user : str, optional 
        User name (default is None, meaning the database is public).
    password : str, optional 
        User password (default is None, meaning there is no password access to database).
    collection : str, optional 
        Database collection name for doc location (default is tasks).
    update_duplicates : bool, optional
        Determines ifduplicates in the database will be updated (default is False, meaning 
        the added doc should not replace an existing doc that is equivalent)
    """             
    conn = pm.MongoClient(host, port, j=True)
    db = conn[database]
    collection = db[collection]
    
    if doc and update_duplicates:
        if collection.find(doc).count()==1:
            collection.update_one(doc, {"$set":doc, "$currentDate":{"lastModified":True}})
        elif collection.find(doc).count()>1:
            print("database not updated because more than one file fits this description")
        else:
            collection.insert_one(doc)
    else:
        collection.insert_one(doc)

def update_doc(existing_doc, added_values, host="127.0.0.1", port=27017, 
               database="shearing_simulations", user=None, password=None, collection="tasks"):
    """updates a single document in a database

    Parameters
    ----------
    existing_doc : dict
        Document being updated in a collection
    added_values : dict
        Fields being updated in a document
    host : str, optional
        Database connection host (the default is 127.0.0.1, or the local computer being used)
    port : int, optional
        Database host port (default is 27017, which is the pymongo default port).
    database : str, optional
        Name of the database being used (default is shearing_simulations).
    user : str, optional 
        User name (default is None, meaning the database is public).
    password : str, optional 
        User password (default is None, meaning there is no password access to database).
    collection : str, optional 
        Database collection name for doc location (default is tasks).
    """
    
    conn = pm.MongoClient(host, port, j=True)
    db = conn[database]
    collection = db[collection]

    if collection.find(existing_doc).count()==1:
        collection.update_one(existing_doc, {"$set":added_values})
    else:
        print("database not updated because there is no existing doc or there are more than one existing docs found")

def query_sim(host="127.0.0.1", port=27017, database="shearing_simulations", user=None,
              password=None, collection="tasks", **kwargs):
    """queries a database collection for documents that fit the keyword arguements

    Parameters
    ----------
    host : str, optional
        Database connection host (the default is 127.0.0.1, or the local computer being used)
    port : int, optional
        Database host port (default is 27017, which is the pymongo default port).
    database : str, optional
        Name of the database being used (default is shearing_simulations).
    user : str, optional 
        User name (default is None, meaning the database is public).
    password : str, optional 
        User password (default is None, meaning there is no password access to database).
    collection : str, optional 
        Database collection name for doc location (default is tasks).
    **kwargs : keys of lists of strings (ie {"A": ["a"], "B": ["b", "bb"]} or A=["a"])
        Fields and field values being queried in a simulation.
    
    Returns
    -------
    cursor : pymongo.Cursor()
        An iterable python object that contains all the documents that the query specifies.
    """
    client = pm.MongoClient(host, port, j=True)
    db = client[database]
    collection = db[collection]

    keys = list()
    fields = list()
    for key, doc_fields in kwargs.items():
        for field in doc_fields:
            keys.append(key)
            fields.append(field)
    
    it_len = len(keys)
    if not keys:
        raise ValueError('**kwargs needed')
    elif it_len == 1:
        cursor = collection.find({keys[0]:fields[0]})
    else:
        and_list = list()
        or_list = list()
        key_hold = keys[0]
        for i in range(it_len):
            if i == it_len-1 and key_hold != keys[i]:
                and_list.append({"$or":or_list})
                or_list = [{keys[i]:fields[i]}]
                and_list.append({"$or":or_list})
            elif key_hold is not keys[i]:
                and_list.append({"$or":or_list})
                or_list = [{keys[i]:fields[i]}]
            elif i == it_len-1:
                or_list.append({keys[i]:fields[i]})
                and_list.append({"$or":or_list})
            else:
                or_list.append({keys[i]:fields[i]})
            key_hold = keys[i]
    
        cursor = collection.find({"$and":and_list})
    return cursor

def retrieve_all(host="127.0.0.1", port=27017, database="shearing_simulations", user=None,
                 password=None, collection="tasks"):
    """retrieves all the documents in a database collection

    Parameters
    ----------
    host : str, optional
        Database connection host (the default is 127.0.0.1, or the local computer being used)
    port : int, optional
        Database host port (default is 27017, which is the pymongo default port).
    database : str, optional
        Name of the database being used (default is shearing_simulations).
    user : str, optional 
        User name (default is None, meaning the database is public).
    password : str, optional 
        User password (default is None, meaning there is no password access to database).
    collection : str, optional 
        Database collection name for doc location (default is tasks).
    
    Returns
    -------
    cursor : pymongo.Cursor()
        An iterable python object that contains all the documents that the query specifies.
    """
    client = pm.MongoClient(host, port, j=True)
    db = client[database]
    collection = db[collection]   
 
    cursor = collection.find({})
    return cursor

def get_uri(name):
    """returns the full path name including computer ip location for a file or directory
    Parameters
    ----------
    name : str
        file or directory name
    
    Returns
    -------
    full_uri : str
        full file or directory name
    """   
    fullpath = os.path.abspath(name)
    try:
        hostname = socket.gethostbyaddr(socket.gethostname())[0]
    except:
        hostname = socket.gethostname()
    full_uri = "{}:{}".format(hostname, fullpath)
    return full_uri
