from myconnection import *


if __name__ == '__main__':
    
    cnx = connect_to_mysql(attempts=3)
    
    if cnx and cnx.is_connected():
        
        cursor = create_cursor(cnx)
        
        #print(type(cursor))
        
        create_database(cursor, 'dp_teste')
        
        create_product_table(cursor, 'dp_teste', 'tb_produtos')
        
        show_tables(cursor, 'dp_teste')
        
        df = read_csv('/home/noah/Documentos/pipeline-python-mongodb-mysql/data/tabela_produtos.csv')
        
        add_product_data(cnx, cursor, df, 'dp_teste', 'tb_produtos')
        

        cnx.close() 
        
    else:
        print("Could not connect")
        
