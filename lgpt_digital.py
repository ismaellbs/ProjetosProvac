import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import fitz  # PyMuPDF
from urllib.parse import quote_plus
import os
import uuid  # Para gerar UUIDs

senha_codificada = quote_plus("Provac@2024")

DATABASE_URI = f'postgresql+psycopg2://admin_provac:{senha_codificada}@192.168.0.232:5432/provac_producao'

def conect_protgress():
    conn = psycopg2.connect(
        database="provac_producao",
        user="admin_provac",
        password="Provac@2024",
        host='192.168.0.232',
        port=5432
    )
    return conn, conn.cursor() 

def get_connection():
    engine = create_engine(DATABASE_URI)
    return engine.connect()

def fetch_user_data(cpf, filial):
    query = f"""
        SELECT ra_filial, ra_mat, ra_nome, ra_admissao, ra_cc, ct_desccc, ra_depto, r6_deptodesc, ra_cic
        FROM protheus.sra WHERE ra_cic = '{cpf}' AND ra_filial = '{filial}'
    """
    conn = get_connection()
    result = pd.read_sql(query, conn)
    conn.close()
    return result

def save_image(file, directory="uploads"):
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_id = str(uuid.uuid4())
    file_ext = os.path.splitext(file.name)[-1]
    file_path = os.path.join(directory, f"{file_id}{file_ext}")
    
    with open(file_path, "wb") as f:
        f.write(file.read())
    
    return file_path

def display_pdf(file_path):
    document = fitz.open(file_path)
    num_pages = document.page_count
    
    for page_num in range(num_pages):
        page = document.load_page(page_num)
        pix = page.get_pixmap()
        img = pix.tobytes("png")
        st.image(img, caption=f"Página {page_num + 1}")

def check_cpf_exists(cpf):
    conn, cur = conect_protgress()
    try:
        query = "SELECT 1 FROM rh.aceite_lgpd WHERE cpf = %s"
        cur.execute(query, (cpf,))
        result = cur.fetchone()
        return result is not None
    except psycopg2.Error as e:
        print(f"Erro ao verificar CPF: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def insert_data(matricula, nome, dt_admissao, cc, desc_cc, depto, desc_depto, photo_path, termo_aceite, cpf):
    conn, cur = conect_protgress()
    try:
        query = """
            INSERT INTO rh.aceite_lgpd (matricula, nome, dt_admissao, cc, desc_cc, depto, desc_depto, foto, termo_aceite, cpf)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(query, (matricula, nome, dt_admissao, cc, desc_cc, depto, desc_depto, photo_path, termo_aceite, cpf))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Ocorreu um erro ao inserir dados: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def main_page():
    st.title("Sistema de Consulta de Funcionários")

    grupo = st.selectbox("Selecione o Grupo da Empresa", ["GRUPO PROVAC", "GRUPO AEON"])

    if grupo == "GRUPO PROVAC":
        filial = "0101"
        pdf_file_path = "/Users/macismael/Downloads/Termo LGPD -2024 - Provac.pdf"
    else:
        filial = "0201"
        pdf_file_path = "/Users/macismael/Downloads/Termo LGPD -2024 - AEON.pdf"

    cpf = st.text_input("Insira o CPF do Funcionário")
    data = fetch_user_data(cpf, filial)
    if not data.empty:
        matricula = data['ra_mat'].iloc[0]
        nome = data['ra_nome'].iloc[0]
        dt_admissao = data['ra_admissao'].iloc[0]
        cc = data['ra_cc'].iloc[0]
        desc_cc = data['ct_desccc'].iloc[0]
        depto = data['ra_depto'].iloc[0]
        desc_depto = data['r6_deptodesc'].iloc[0]
        
        st.write("Matrícula:", matricula)
        st.write("Nome:", nome)
        st.write("Data de Admissão:", dt_admissao)
        st.write("Centro de Custo:", cc)
        st.write("Nome do Centro de Custo:", desc_cc)
        st.write("Departamento:", desc_depto)

        st.subheader("Upload da Foto do Funcionário")
        st.subheader("Exemplo de Foto")
        exemplo_img_path = "/Users/macismael/Documents/MyProject/ProjetosProvac/Model/Foto_modelo.png"
        st.image(exemplo_img_path, caption='Foto Exemplo', width=200)

        uploaded_file = st.file_uploader("Tire ou envie uma foto do funcionário", type=["jpg", "jpeg", "png"])
        
        st.subheader("Termo de Contrato")
        st.write("Por favor, leia o termo do contrato abaixo:")
        display_pdf(pdf_file_path)
        
        aceite = st.checkbox("Eu aceito os termos do contrato")

        if st.button("Enviar"):
            if aceite and cpf and uploaded_file is not None:
                if check_cpf_exists(cpf):
                    st.error("CPF já cadastrado. Não é possível inserir duplicado.")
                else:
                    photo_path = save_image(uploaded_file)
                    insert_data(
                        str(matricula), nome, str(dt_admissao), cc, desc_cc, depto, desc_depto, 
                        photo_path, termo_aceite=str(aceite), cpf=str(cpf)
                    )
                    st.success("Dados enviados com sucesso!")
            else:
                st.error("Por favor, preencha todos os campos obrigatórios: aceite os termos, forneça um CPF e carregue uma foto do funcionário.")
    else:
        st.write("Nenhum dado encontrado para o CPF informado.")

main_page()