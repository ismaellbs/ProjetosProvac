import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus
senha_codificada = quote_plus("Provac@2024")
engine = create_engine(f'postgresql+psycopg2://admin_provac:{senha_codificada}@192.168.0.232:5432/provac_producao')

#Conectar ao banco do PostgreSQL
def conect_protgress():
    import psycopg2

    conn = psycopg2.connect(database = "provac_producao", 
                        user = "admin_provac", 
                        host= '192.168.0.232',
                        password = "Provac@2024",
                        port = 5432)
    cur = conn.cursor()

    return conn, cur

def get_max_date_from_db(conn):
    print("Obtendo a data máxima do banco de dados...")
    connection = conn
    cursor = connection.cursor()
    query = "SELECT MAX(data_referencia) FROM departamento_pessoal.folha"
    cursor.execute(query)
    max_date = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    print(f"Data máxima obtida: {max_date}")
    return max_date

conn, cur = conect_protgress()
max_date = get_max_date_from_db(conn)


def list_new_xlsx_files(folder_path, max_date):
    print(f"Listando novos arquivos .xlsx no caminho: {folder_path}...")
    new_files = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.xlsx'):
            try:
                file_date = datetime.strptime(filename.split('.')[0], '%Y-%m-%d').date()
                print(f"Arquivo encontrado: {filename} com data {file_date}")
                if max_date is None or file_date > max_date:
                    print(f"Arquivo {filename} é novo e será processado.")
                    new_files.append(filename)
                else:
                    print(f"Arquivo {filename} não é mais recente que a data máxima.")
            except ValueError:
                print(f"Formato de data inválido no arquivo {filename}. Ignorando arquivo.")
                continue
    print(f"Arquivos novos a serem processados: {new_files}")
    return new_files

def get_db_columns(conn, table_name, schema_name):
    print(f"Obtendo colunas da tabela {schema_name}.{table_name} do banco de dados...")
    connection = conn
    cursor = connection.cursor()
    query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
    cursor.execute(query)
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    print(f"Colunas obtidas do banco de dados: {columns}")
    return columns

def convert_to_date(ref_value):
    # Converte o valor do formato YYYYMM para o primeiro dia do mês
    if pd.notna(ref_value):
        try:
            year = int(ref_value // 100)
            month = int(ref_value % 100)
            return datetime(year, month, 1).date()
        except ValueError:
            print(f"Formato de data inválido: {ref_value}.")
            return None
    return None

def insert_in_chunks(df, engine, schema, table_name, chunksize=1000):
    total_rows = len(df)
    for i in range(0, total_rows, chunksize):
        chunk = df.iloc[i:i+chunksize]
        print(f"Inserindo linhas {i+1} a {i+len(chunk)} de {total_rows} no banco de dados...")
        chunk.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)
        print(f"Linhas {i+1} a {i+len(chunk)} inseridas com sucesso.")

def read_and_insert_xlsx_files(folder_path, filenames, engine, db_columns):
    print("Iniciando o processo de leitura e inserção dos arquivos .xlsx no banco de dados...")
    engine = engine
    for filename in filenames:
        try:
            file_path = os.path.join(folder_path, filename)
            print(f"Lendo arquivo: {file_path}")
            df = pd.read_excel(file_path)

            print(f"Arquivo: {filename}, Colunas do DataFrame: {df.columns.tolist()}")
            print(f"Colunas esperadas no banco de dados: {db_columns}")

            # Mapeando as colunas do DataFrame para as colunas do banco de dados
            column_mapping = {
                'Filial': 'filial',
                'Matricula': 'matricula',
                'Funcionário': 'funcionario',
                'Admissão': 'admissao',
                'CPF': 'cpf',
                'Função': 'funcao',
                'Salário': 'salario',
                'Status': 'status',
                'Sindicato': 'sindicato',
                'Descrição Sindicato': 'descricao_sindicato',
                'Turno': 'turno',
                'Carga Horária': 'carga_horaria',
                'Cód CC': 'cod_cc',
                'Descrição Centro de Custo': 'descricao_centro_de_custo',
                'Verba': 'verba',
                'Descrição Verba': 'descricao_verba',
                'Referencia': 'referencia',
                'Valor': 'valor',
                'Bco/Agência': 'bco_agencia',
                'Conta Corrente': 'conta_corrente',
                'Depto': 'depto',
                'Descrição Depto': 'descricao_dpto',
                'PERIODO DA FOLHA': 'data_referencia'
            }

            df.rename(columns=column_mapping, inplace=True)

            if 'data_referencia' in df.columns:
                df['data_referencia'] = df['data_referencia'].apply(convert_to_date)
                print(f"Coluna data_referencia formatada com sucesso.")

            # Ajuste de colunas no DataFrame
            df = df[df.columns.intersection(db_columns)]  # Mantém apenas as colunas que existem no banco de dados
            missing_columns = set(db_columns) - set(df.columns)
            for col in missing_columns:
                df[col] = None  # Adiciona colunas que estão faltando no DataFrame

            df = df[db_columns]  # Garante que o DataFrame tem as colunas na ordem correta

            print(f"Colunas finais do DataFrame após ajustes: {df.columns.tolist()}")

            if len(df.columns) != len(db_columns):  # Verifica se o número de colunas coincide
                raise ValueError(f"Length mismatch: Expected axis has {len(db_columns)} elements, but DataFrame has {len(df.columns)} elements")

            if 'cpf' in df.columns:
                df['cpf'] = df['cpf'].apply(lambda x: str(x).zfill(11))
                print(f"Coluna CPF formatada com sucesso.")

            # Inserir em chunks
            insert_in_chunks(df, engine, schema='departamento_pessoal', table_name='folha', chunksize=1000)

        except Exception as e:
            print(f"Erro ao processar o arquivo {filename}: {str(e)}")

print("Data máxima: ", max_date)

folder_path = '//home//automacao//pasta_rede//DP//FOLHAS'  # Caminho especificado no código original
new_files = list_new_xlsx_files(folder_path, max_date)

conn, cur = conect_protgress()

db_columns = get_db_columns(conn, 'folha', 'departamento_pessoal')

conn, cur = conect_protgress()
read_and_insert_xlsx_files(folder_path, new_files, engine, db_columns)
