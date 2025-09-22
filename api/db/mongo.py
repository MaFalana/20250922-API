'''
Name: DB.py
Description: Database connection manager for
'''

from pymongo import MongoClient
from dotenv import load_dotenv

from azure.identity import DefaultAzureCredential
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
import os


load_dotenv(".env") # Load environment variables from .env file

class DatabaseManager(object):
    def __init__(self):
        pass

class MongoManagaer(DatabaseManager):
    def __init__(self, db_name):
        user = os.getenv("MONGO_USER")
        pwd = os.getenv("MONGO_PASS")
        host = os.getenv("MONGO_HOST")
        connection = f"mongodb+srv://{user}:{pwd}@{host}/"
        
        self.client = MongoClient(connection)
        self.db = self.client[db_name]
        print(f'Connected to {db_name} database') 
        self.manga = self.db['Manga'] # Get the Manga collection from the database

    def query(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.find_one(query)

    def insert(self, collection_name, document):
        collection = self.db[collection_name]
        collection.insert_one(document)
        #result = collection.insert_one(document)
        #return result.inserted_id

    def __del__(self):
        self.client.close()
        


    def addManga(self, manga): # Creates a new manga in the database - CREATE
        if self.exists('Manga', manga): # Query the database to see if the manga already exists
            self.updateManga(manga) # If it does, update the manga
            print(f"Updated manga: {manga['title']} from {manga['source']}")
        else:
            self.manga.insert_one(manga)  # If it doesn't, add the manga
            print(f"Added manga: {manga['title']} from {manga['source']}")


    def getManga(query): # Gets a manga from the database - READ
        manga = self.manga.query('Manga',query)
        print(f"Found manga: {manga['title']}")
        return manga


    def updateManga(self, manga): # Updates a manga in the database - UPDATE
        # if cover is different or number of chapters is different, update
        self.manga.update_one({'id': manga['id']}, {'$set': manga})
        print(f"Updated manga: {manga['title']} from {manga['source']}")


    def deleteManga(self, manga): # Deletes a manga from the database - DELETE
        self.manga.delete_one({'_id': manga['_id']})
        print(f"Deleted manga: {manga['title']} from {manga['source']}")



    def exists(self, collection_name, query): # Checks if a document exists in the database, return boolean
        collection = self.db[collection_name]
        return collection.find_one(query) != None
    
class AzureCosmosManager(DatabaseManager):
    def __init__(self, endpoint, key, database_name):

        # Authenticate the client
        self.credential = DefaultAzureCredential()
        self.client = CosmosClient(url="<azure-cosmos-db-nosql-account-endpoint>", credential=self.credential)

        self.database = self.client.get_database_client("mysideprojects") # Get a database
        self.container = self.database.get_container_client("potree") # Get a container

    def query(self, container_name, query):
        self.container = self.database.get_container_client(container_name)
        return list(self.container.query_items(query=query, enable_cross_partition_query=True))
    
    def insert(self, container_name, document):
        container = self.database.get_container_client(container_name)
        container.create_item(body=document)

    def __del__(self):
        pass

    def addProject(self, project):
        if self.exists('Project', project): # Query the database to see if the project already exists
            self.updateProject(project) # If it does, update the project
            print(f"Updated project: {project['title']} with {project['source']}")
        else:
            self.container.upsert_item(project)  # If it doesn't, add the project
            print(f"Added project: {project['title']} with {project['source']}")

    def getProject(self, query):
        existing_item = self.container.read_item(item=query, partition_key="gear-surf-surfboards",)
        

    def updateProject(self, project):
        pass

    def deleteProject(self, project):
        pass

class AzureMongoManager(DatabaseManager):
    def __init__(self, db_name):
        self.db_name = db_name
        connection = os.getenv("MONGO_CONNECTION_STRING")
        self.client = MongoClient(connection)
        self.db = self.client[db_name]
        print(f'Connected to {db_name} database') 
        self.objects = self.db['Photos'] # Get the Photos collection from the database

    def query(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.find_one(query)

    def insert(self, collection_name, document):
        collection = self.db[collection_name]
        collection.insert_one(document)
        #result = collection.insert_one(document)
        #return result.inserted_id

    def __del__(self):
        self.client.close()


    def exists(self, collection_name, query): # Checks if a document exists in the database, return boolean
        return self.query(collection_name, query) != None
        


    def addObject(self, object): # Creates a new object in the database - CREATE
        if self.exists(self.db_name, object): # Query the database to see if the object already exists
            self.updateObject(object) # If it does, update the project
            print(f"Updated objects: {object['_id']}")
        else:
            self.objects.insert_one(object)  # If it doesn't, add the project
            print(f"Added object: {object['_id']}")


    def getObject(self, query): # Gets an object from the database - READ
        filter = {"_id": query}
        object = self.objects.find_one(filter)
        print(f"Found object: {object['_id']}")
        return object


    def updateObject(self, id, filter): # Updates a object in the database - UPDATE
        self.objects.update_one({'_id': id}, filter)
        object = self.getObject(id)
        print(f"Updated object: {object['_id']}")


    def deleteObject(self, id): # Deletes an object from the database - DELETE
        object = self.getObject(id)
        self.objects.delete_one({'_id': id})
        print(f"Deleted project: {object['_id']}")

class AzureBlobManager:
    def __init__(self, connection_string, container_name):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        print(f'Connected to Azure Blob Storage container: {container_name}')

    def upload_file(self, file_path, blob_name):
        with open(file_path, "rb") as data:
            self.container_client.upload_blob(name=blob_name, data=data)
            print(f"Uploaded {blob_name} to Azure Blob Storage")

    def download_file(self, blob_name, download_path):
        with open(download_path, "wb") as download_file:
            download_stream = self.container_client.download_blob(blob_name)
            download_file.write(download_stream.readall())
            print(f"Downloaded {blob_name} from Azure Blob Storage to {download_path}")

    def delete_file(self, blob_name):
        self.container_client.delete_blob(blob_name)
        print(f"Deleted {blob_name} from Azure Blob Storage")