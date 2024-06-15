from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd

def visualize_collection(col):
    #leitura de todos os dados da coleção
    for doc in col.find():
        print(doc)
        
def rename_column(col, col_name, new_name):
    #renomeia uma coluna existente.
    col.update_many({},{"$rename" : {"lat": "Latitude", "lon" : "Longitude"}})
    
def select_category(col, category):
    #seleciona documentos que correspondam a uma categoria específica.
    query = {"Categoria do Produto": "livros"}
    category_list = []
    for doc in col.find(query):
        category_list.append(doc)
        
    return category_list

def make_regex(col, regex):
    #seleciona documentos que correspondam a uma expressão regular específica.
    query = {"Categoria do Produto": "livros"} #Filtrando categoria livros
    regex_list = []
    for doc in col.find(query):
        regex_list.append(doc)
    
    return regex_list

def create_dataframe(lista):
    # cria um dataframe a partir de uma lista de documentos.
    df = pd.DataFrame(lista)
    
    return df

def format_date(df):
    #formata a coluna de datas do dataframe para o formato "ano-mes-dia".
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')

def save_csv(df, path):
    #salva o dataframe como um arquivo CSV no caminho especificado.
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")
    

if __name__ == "__main__":

    # estabelecendo a conexão e recuperando os dados do MongoDB
    client = connect_mongo("sua_uri")
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    # renomeando as colunas de latitude e longitude
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # salvando os dados da categoria livros
    lst_livros = select_category(col, "livros")
    df_livros = create_dataframe(lst_livros)
    format_date(df_livros)
    save_csv(df_livros, "../data_teste/tb_livros.csv")

    # salvando os dados dos produtos vendidos a partir de 2021
    lst_produtos = make_regex(col, "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "../data_teste/tb_produtos.csv")






