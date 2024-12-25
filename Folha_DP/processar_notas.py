import os
import re
import shutil
from PyPDF2 import PdfReader, PdfWriter
import pandas as pd
import PyPDF2
from collections import defaultdict
import pymssql
from glob import glob
from datetime import datetime
from sqlalchemy import create_engine
pd.set_option('display.max_column', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
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


def sanitize_filename(filename):
    # Replace invalid characters with underscores
    invalid_chars = '/\\:*?"<>.-'
    for char in invalid_chars:
        filename = filename.replace(char, '')
        filename = filename.replace(' ', '')
    return filename.strip()

# Função para ler e extrair texto de um PDF
def ler_extrair_pdf(pdf_file):
    reader = PdfReader(pdf_file)
    page_texts = [page.extract_text()
                  for page in reader.pages if page.extract_text()]
    return page_texts, reader

def inserir_no_banco(data_list, engine):
    df = pd.DataFrame(data_list)
    # Definir o nome da tabela e o esquema conforme necessário
    df.to_sql('comprovantes', engine, if_exists='append',index=False, schema='departamento_pessoal')

# Função para extrair informações de um comprovante
def extrair_informacoes(receipt):
    def sanitize_filename(filename):
        # Replace invalid characters with underscores
        invalid_chars = '/\\:*?"<>.-\s+'
        for char in invalid_chars:
            filename = filename.replace(char, '')
        return filename.strip()
        # Adaptar as expressões regulares conforme necessário
    valor = re.search(r'VALOR: +([\d\.,]+)', receipt).group(1)
    return {
        'pagador': sanitize_filename(re.search(r'PAGADOR: (.+?)\n', receipt).group(1)),
        'agencia_pagador': re.search(r'AGENCIA: (\d+) +CONTA', receipt).group(1),
        'conta_pagador': re.search(r'CONTA: ([\d\.-]+)\nNR\. DOCUMENTO', receipt).group(1),
        'nr_documento_pagador': re.search(r'NR\. DOCUMENTO:\s*(\d*)', receipt).group(1),
        'beneficiario': re.search(r'BENEFICIARIO: +(.+?)\n', receipt).group(1).strip(),
        'cpf_cnpj': sanitize_filename(re.search(r'CPF/CNPJ: +([\d\.-]+)', receipt).group(1)),
        'agencia_beneficiario': re.search(r'BENEFICIARIO:.*?AGENCIA: (\d+)', receipt, re.DOTALL).group(1),
        'conta_beneficiario': re.search(r'BENEFICIARIO:.*?CONTA: ([\d\.-]+)', receipt, re.DOTALL).group(1),
        'data_pagamento': re.search(r'DATA DO PAGAMENTO: +([\d/]+)', receipt).group(1),
        'valor': valor.replace('.', '').replace(',', '.'),
        'nr_autenticacao': sanitize_filename(re.search(r'NR. AUTENTICACAO: +([\w\.]+)', receipt).group(1)),
        'nr_documento_recebedor': re.search(r'BENEFICIARIO:.*?NR\. DOCUMENTO:\s*(\d*)', receipt, re.DOTALL).group(1)}

# Função para criar um PDF individual para cada página
def criar_pdf_individual(folder_path, page, nome_arquivo):
    if not os.path.exists(os.path.join(folder_path, nome_arquivo)):
        writer = PdfWriter()
        writer.add_page(page)
        with open(os.path.join(folder_path, nome_arquivo), 'wb') as output_pdf:
            writer.write(output_pdf)
        return True


folder_path = '/home/automacao/pasta_rede/DP/COMPROVANTES/BANCO_BRASIL/'
cont = 0
for filename in os.listdir(folder_path):
    # logger.info(folder_path, filename)
    if filename.endswith('.pdf') and filename.count('_') != 3:
        pdf_file = os.path.join(folder_path, filename)
        page_texts, reader = ler_extrair_pdf(pdf_file)

        # Ignorar o arquivo se ele tiver apenas uma página
        if len(reader.pages) == 1:
            print(f"Arquivo de uma pagina: {pdf_file}")
            continue

        dados_comprovantes = []
        for page_text, page in zip(page_texts, reader.pages):
            if 'COMPROVANTE' in page_text:
                try:
                    data = extrair_informacoes(page_text)
                    nome_arquivo = f"{data['beneficiario']}_{data['cpf_cnpj']}_{ data['data_pagamento']}_{data['nr_autenticacao']}"
                    nome_arquivo = sanitize_filename(nome_arquivo) + '.pdf'
                    criar_registro = criar_pdf_individual(folder_path, page, nome_arquivo)
                    print(nome_arquivo)
                    print(data)
                    # Acumular os dados para inserção no banco
                    if criar_registro == True:
                        dados_comprovantes.append(data)
                except Exception as e:
                    print(f"Erro ao extrair as informacoes: {e}")

    # Inserir no banco de dados
    conn, cur = conect_protgress()

    inserir_no_banco(dados_comprovantes, engine)
    #df = pd.DataFrame(dados_comprovantes)
    #df.to_csv(f'/home/automacao/pasta_rede/DP/dados_comprovantes{cont}.csv', index=False)

    # Após processar todas as páginas do PDF
    #os.remove(pdf_file)
    
    cont +=1


