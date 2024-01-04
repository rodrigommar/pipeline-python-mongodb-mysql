from extract_and_save_data import *
import pandas as pd


#  imprime todos os documentos existentes na coleção.
def visualize_collection(col):
    
    query = {}
    projection =  {"_id": 0, "Produto": 1, "Data da Compra": 1, "Vendedor": 1, 'Valor do Frete': 1}
    
    cursor = col.find(query, projection)
    
    for doc in cursor:
        print(doc)
    
    
# renomeia uma coluna existente. 
def rename_column(col, col_name, new_name):

    col.update_many({}, {"$rename": {col_name: new_name}})
    
    print(f'Coluna antiga: {col_name} \n Coluna nova: {new_name}')

 
# seleciona documentos que correspondam a uma categoria específica.  
def select_category(col, category):
    
    query = {'Categoria do Produto': category }
    projection = {"_id": 0, 'Categoria do Produto': 1, "Produto": 1, "Data da Compra": 1}
    
    cursor  = col.find(query, projection)
    
    for doc in cursor:
        print(doc)


# seleciona documentos que correspondam a uma expressão regular específica.
def make_regex(col, regex):
    
    query = {'Data da Compra': {"$regex": f"{regex}"}}
    
    projection =  {
        '_id': 1,
        'Produto': 1,
        'Categoria do Produto': 1,
        'Preço': 1,
        'Valor do Frete': 1,
        'Data da Compra': 1
    }
    
    lista_doc = []
    cursor = col.find(query, projection)
    
    for doc in cursor:
        lista_doc.append(doc)
    
    return lista_doc
    

# cria um dataframe a partir de uma lista de documentos.
def create_dataframe(lista):
    
    df = pd.DataFrame(lista)
    
    return df
    
    
# formata a coluna de datas do dataframe para o formato "ano-mes-dia".
def format_date(df):
    
    df['Data da Compra'] = pd.to_datetime(df['Data da Compra'], format=('%d/%m/%Y'))
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')
    
    return df


# salva o dataframe como um arquivo CSV no caminho especificado.
def save_csv(df, path):
    
    if df.to_csv(path, index=False):
    
        print('Success')
    
    

if __name__ == '__main__':
    
    # criando um objeto do tipo CreateConectionMongoDb
    connect = CreateConectionMongoDb
    
    uri = "mongodb+srv://rodrigommar:13098317@cluster-pipeline.pokmnwf.mongodb.net/?retryWrites=true&w=majority"

    # instanciando um objeto do tipo MongoClient para acessar database e collection
    client = connect.connect_mongodb(uri)
    
    db_pipeline = connect.create_connect_db(client, 'db_pipeline')
    
    collection_products = connect.create_connect_collection(db_pipeline, 'collection_products')
    
    print(type(collection_products), type(db_pipeline))

    
    rename_column(collection_products, 'Frete', 'Valor do Frete')
    

    regex_ = "/202[1-9]"
    
    lista_produtos_caros = make_regex(collection_products, regex_)
    
    df = create_dataframe(lista_produtos_caros)
    
    print(df.head())
    
    df = format_date(df)
    
    print(df.head())
    
    save_csv(df, './data/tabela_produtos_v2.csv')