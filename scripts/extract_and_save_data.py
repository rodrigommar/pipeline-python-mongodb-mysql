from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests


class CreateConectionMongoDb:
    
    
    def __init__(self) -> None:
        
        self.uri = None
        self.client = None
        self.database_name = None
        self.collection_name = None
    
    
    def connect_mongodb(uri):
        
        # Create a new client and connect to the server
        client = MongoClient(uri, server_api=ServerApi('1'))
        
        # Send a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            print('"Pinged your deployment. You successfully connected to MongoDB!"')
        except Exception as e:
            print(e)
            
        return client


    def create_connect_db(client, db_name):
            
        return client[db_name]
            
             
    def create_connect_collection(db, collection_name):
        
        connection_list = db.list_collection_names()

        if collection_name in connection_list:
            
            return db[collection_name]
        
        else:
            
            collection = db[collection_name]
        
        return collection


    def extract_api_data(url):
        
        response = requests.get(url)
        
        if response.status_code == 200:
            
            dados = response.json()  
            return dados
        
        else:
            print(f'Status code: {response.status_code}')


    def insert_data(collection, data):
        
        docs = collection.insert_many(data)
        
        n_docs_inseridos = len(docs.inserted_ids)
        
        return n_docs_inseridos


if __name__ == '__main__':
    
    uri = "mongodb+srv://rodrigommar:13098317@cluster-pipeline.pokmnwf.mongodb.net/?retryWrites=true&w=majority"


    connection = CreateConectionMongoDb
    
    client = connection.connect_mongodb(uri)
    
    db_pipeline = connection.create_connect_db(client, 'db_pipeline')
    
    collection_products = connection.create_connect_collection(db_pipeline, 'collection_products')
    
    url = 'https://labdados.com/produtos'
    data = connection.extract_api_data(url)
    
    n_docs = connection.insert_data(collection_products, data)
    
    print(type(db_pipeline))
    print(type(collection_products))
    print(type(data), len(data))
    print(n_docs)
    
    client.close()