from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import psycopg2 
#from pymssql import connect
import pandas as pd
import pyodbc

# Codifique a senha do banco de dados
senha_codificada = quote_plus("Provac@2024")

# String de conexão com o banco de dados
DATABASE_URI = f'postgresql+psycopg2://admin_provac:{senha_codificada}@192.168.0.232:5432/provac_producao'

#Conectar ao banco do PostgreSQL
def conect_protgress():
    conn = psycopg2.connect(database = "provac_producao", 
                        user = "admin_provac", 
                        host= '192.168.0.232',
                        password = "Provac@2024",
                        port = 5432)
    cur = conn.cursor()
    return conn, cur

def conectar_protheus():

    # Defina as informações de conexão
    server = '192.168.0.236'  # Substitua pelo endereço do seu servidor
    database = 'PROTHEUS_PRODUCAO'               # Nome do banco de dados
    username = 'ismael.silva'                      # Nome de usuário
    password = 'w!1zayeUAM'                        # Senha

    # String de conexão ODBC
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    return conn, cursor

con_tot, cursor = conectar_protheus()

conn, cur = conect_protgress()


query = """
    SELECT 
        RA_FILIAL AS ra_filial, 
        RA_MAT AS ra_mat, 
        RA_NOME AS ra_nome, 
        RA_ADMISSA AS ra_admissao, 
        RA_CC AS ra_cc, 
        CTT_DESC01 AS ct_desccc, 
        RA_DEPTO AS ra_depto, 
        QB_DESCRIC AS r6_deptodesc, 
        RA_CIC AS ra_cic
    FROM 
        SRA010 
        INNER JOIN CTT010 ON CTT_CUSTO = RA_CC AND CTT_FILIAL = RA_FILIAL
        INNER JOIN SQB010 ON QB_DEPTO = RA_DEPTO AND QB_CC = RA_CC
    WHERE
        SRA010.D_E_L_E_T_ <> '*'AND
        CTT010.D_E_L_E_T_ <> '*'AND
        SQB010.D_E_L_E_T_ <> '*' AND
        SRA010.RA_SITFOLH <> 'D'
    GROUP BY
        RA_FILIAL, 
        RA_MAT, 
        RA_NOME, 
        RA_ADMISSA, 
        RA_CC, 
        CTT_DESC01, 
        RA_DEPTO, 
        QB_DESCRIC, 
        RA_CIC      
"""

query_sra = "SELECT ra_mat, ra_cic FROM protheus.sra"

df = pd.read_sql(query, con_tot)

sra = pd.read_sql(query_sra, conn)
sra.rename(columns={'ra_cic':'cpf'}, inplace=True)

df = pd.merge(df, sra, how='left', on = 'ra_mat')
df = df[df['cpf'].isnull()]

def insert_data(ra_filial, ra_mat, ra_nome, ra_admissao, ra_cc, ct_desccc, ra_depto, r6_deptodesc, ra_cic):
    conn, cur = conect_protgress()
    query = """
        INSERT INTO protheus.sra (ra_filial, ra_mat, ra_nome, ra_admissao, ra_cc, ct_desccc, ra_depto, r6_deptodesc, ra_cic)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Passando os parâmetros como um dicionário
    cur.execute(query, (ra_filial, ra_mat, ra_nome, ra_admissao, ra_cc, ct_desccc, ra_depto, r6_deptodesc, ra_cic))
    conn.commit()
    
    cur.close()
    conn.close()

for i, v in df.iterrows():
    ra_filial = v['ra_filial'] 
    ra_mat =  v['ra_mat']
    ra_nome =  v['ra_nome']
    ra_admissao =  v['ra_admissao'] 
    ra_cc =  v['ra_cc'] 
    ct_desccc =  v['ct_desccc'] 
    ra_depto =  v['ra_depto']
    r6_deptodesc =  v['r6_deptodesc'] 
    ra_cic =  v['ra_cic']

    insert_data(ra_filial, ra_mat, ra_nome, ra_admissao, ra_cc, ct_desccc, ra_depto, r6_deptodesc, ra_cic)
    print(ra_filial, ra_mat, ra_nome, ra_admissao, ra_cc, ct_desccc, ra_depto, r6_deptodesc, ra_cic)
print('Dados Inserido com Sucesso')