import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode
import logging
import time
import pandas as pd


# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# Log to console
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


# Also log to a file
file_handler = logging.FileHandler("cpy-errors.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# Connect to database dbprodutos in Mysql
def connect_to_mysql(attempts=3, delay=2):
    attempt = 1
    
    # Carrega as variáveis do arquivo .env no ambiente de trabalho
    load_dotenv()

    # A função os.getenv é usada para obter o valor das variáveis de ambiente
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USERNAME")
    pswd = os.getenv("DB_PASSWORD")

    # implement a reconnection routine
    while attempt < attempts + 1:
        
        try:
            
            return mysql.connector.connect(
               host=host,
                user=user,
                password=pswd,
                database='dbprodutos', 
            )
            
        except (mysql.connector.Error, IOError) as err:
            
            if (attempts is attempt):
            
                # Attempts to reconnect failed; returning None
                logger.info("Failed to connect, exiting without a connection: %s", err)
                return None
            logger.info(
                "Connection failed: %s. Retrying (%d/%d)...",
                err,
                attempt,
                attempts-1,
            )
            
            # progressive reconnect delay
            time.sleep(delay ** attempt)
            attempt += 1
    return None



# cria e retorna um cursor, que serve para conseguirmos executar os comandos SQL,
# utilizando a conexão fornecida como argumento.
def  create_cursor(cnx):
    
    cursor = cnx.cursor()
    
    return cursor


# exibe todos os bancos de dados existentes.
def show_databases(cursor):
    
    cursor.execute('SHOW DATABASES;')
    for db in cursor:
        print(db)


# cria um banco de dados com o nome fornecido como argumento.
def  create_database(cursor, db_name):
    
    cursor.execute('CREATE DATABASE IF NOT EXISTS {};'.format(db_name))
    
    show_databases(cursor)


# cria uma tabela com o nome fornecido no banco de dados especificado.
# A tabela deve ter as colunas que correspondam aos dados que serão inseridos posteriormente.
def create_product_table(cursor, db_name, tb_name): 
    
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
        id VARCHAR(100),
        Produto VARCHAR(100),
        Categoria_Produto VARCHAR(100),
        Preco FLOAT(10,2),
        Frete FLOAT(10,2),
        Data_Compra DATE,
        Vendedor VARCHAR(100),
        Local_Compra VARCHAR(100),
        Avaliacao_Compra INT,
        Tipo_Pagamento VARCHAR(100),
        Quantidade_Parcelas INT,
        Latitude FLOAT(10,2),
        Longitude FLOAT(10,2),
        
        PRIMARY KEY (id));
    ''')
    
    print(f'\nTabela: {tb_name} criada')
    

# lista todas as tabelas existentes no banco de dados especificado.    
def show_tables(cursor, db_name):
    
    cursor.execute(f'USE {db_name};')
    
    cursor.execute(f'SHOW TABLES;')
    
    for tb in cursor:
        print(tb)


#  lê um arquivo csv do caminho fornecido e retorna um DataFrame do pandas com esses dados.
def read_csv(path):
    
    df = pd.read_csv(path)
    
    return df
    

# insere os dados do DataFrame fornecido à tabela especificada no banco de dados especificado.
def add_product_data(cnx, cursor, df, db_name, tb_name):
    
    lista_dados = [tuple(row) for _ , row in df.iterrows()]
    
    sql = f'INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    
    cursor.executemany(sql, lista_dados)
    
    print(f"\n {cursor.rowcount} dados foram inseridos na tabela {tb_name}.")

    cnx.commit()

if __name__ == '__main__':
    df = read_csv('/home/noah/Documentos/pipeline-python-mongodb-mysql/data/tabela_produtos.csv')
    print(type(df))