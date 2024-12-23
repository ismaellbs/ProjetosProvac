import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import fitz  # PyMuPDF
from urllib.parse import quote_plus

# Codifique a senha do banco de dados
senha_codificada = quote_plus("Provac@2024")

# String de conexão com o banco de dados
DATABASE_URI = f'postgresql+psycopg2://admin_provac:{senha_codificada}@192.168.0.232:5432/provac_producao'

# Função para conectar ao banco de dados
def get_connection():
    engine = create_engine(DATABASE_URI)
    return engine.connect()

# Função para buscar dados a partir do CPF
def fetch_user_data(cpf):
    query = f"""
        SELECT ra_mat, ra_nome, ra_admissao, ra_cc, ct_desccc, ra_depto, r6_deptodesc
        FROM sua_tabela
        WHERE cpf = '{cpf}'
    """
    conn = get_connection()
    result = pd.read_sql(query, conn)
    conn.close()
    return result

# Função para exibir um PDF
def display_pdf(file_path):
    # Abre o documento PDF
    document = fitz.open(file_path)
    num_pages = document.page_count
    
    for page_num in range(num_pages):
        page = document.load_page(page_num)
        # Renderiza a página como uma imagem (matriz de pixels)
        pix = page.get_pixmap()
        # Converte a matriz de pixels para um formato compatível com Streamlit
        img = pix.tobytes("png")
        # Exibe a imagem em Streamlit
        st.image(img, caption=f"Página {page_num + 1}")

# Função para inserir dados no banco
def insert_data(matricula, nome, dt_admissao, cc, desc_cc, depto, desc_depto, foto, termo_aceite):
    conn = get_connection()
    query = text("""
        INSERT INTO rh.aceite_lgpd (matricula, nome, dt_admissao, cc, desc_cc, depto, desc_depto, foto, termo_aceite)
        VALUES (:matricula, :nome, :dt_admissao, :cc, :desc_cc, :depto, :desc_depto, :foto, :termo_aceite)
    """)
    conn.execute(query, matricula=matricula, nome=nome, dt_admissao=dt_admissao, cc=cc, desc_cc=desc_cc, depto=depto, desc_depto=desc_depto, foto=foto, termo_aceite=termo_aceite)
    conn.close()

# Função principal para exibir o formulário e o termo de contrato
def main_page():
    st.title("Sistema de Consulta de Funcionários")

    # Formulário de entrada para CPF
    cpf = st.text_input("Insira o CPF do Funcionário")
    if st.button("Consultar"):
        data = fetch_user_data(cpf)
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

            # Upload de Foto do Funcionário
            st.subheader("Upload da Foto do Funcionário")
            uploaded_file = st.file_uploader("Tire ou envie uma foto do funcionário", type=["jpg", "jpeg", "png"])
            foto = None
            if uploaded_file is not None:
                foto = uploaded_file.read()
                st.image(uploaded_file, caption='Foto do Funcionário', use_column_width=True)
            
            # Exibição do termo de contrato
            st.subheader("Termo de Contrato")
            pdf_file_path = "/Users/macismael/Downloads/Termo LGPD -2024 - Provac.pdf"
            st.write("Por favor, leia o termo do contrato abaixo:")
            display_pdf(pdf_file_path)
            
            aceite = st.checkbox("Eu aceito os termos do contrato")

            # Botão Enviar
            if st.button("Enviar"):
                if aceite and cpf and uploaded_file is not None:
                    insert_data(matricula, nome, dt_admissao, cc, desc_cc, depto, desc_depto, foto, termo_aceite=aceite)
                    st.success("Dados enviados com sucesso!")
                else:
                    st.error("Por favor, preencha todos os campos obrigatórios: aceite os termos, forneça um CPF e carregue uma foto do funcionário.")
        else:
            st.write("Nenhum dado encontrado para o CPF informado.")

# Execução da página principal
main_page()