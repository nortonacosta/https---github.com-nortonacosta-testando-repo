from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

def connect_mongo(uri):
    # Criando um novo cliente e conectando ao servidor
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Enviando um ping pra conferir se a conexão foi realizada
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_conect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name]
    return collection

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    docs = col.insert_many(data) #inserindo vários objetos na base de dados
    n_docs_inserted = len(docs.inserted_ids)
    return n_docs_inserted


if __name__ == "__main__":
    
    client = connect_mongo("mongodb+srv://nortonacosta:12345@cluster-pipeline.aikjlvj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline")
    db = create_conect_db(client, 'db_produtos_desafio')
    col = create_conect_db(db, 'produtos')
    
    data = extract_api_data('https://labdados.com/produtos')
    print(f'\nQUantidade de dados extraidos: {len(data)}')
    
    n_docs = insert_data(col, data)
    print(f'\nQUantidade de documentos inseridos: {n_docs}')
     
    client.close 
    
    
    
    