import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import fitz  # PyMuPDF
from urllib.parse import quote_plus
senha_codificada = quote_plus("Provac@2024")
# Substitua pela sua string de conexão do banco de dados
DATABASE_URI = 'postgresql+psycopg2://admin_provac:{senha_codificada}@192.168.0.232:5432/provac_producao'

# Conexão com o banco
def get_connection():
    engine = create_engine(DATABASE_URI)
    return engine.connect()

# Função para buscar dados a partir do CPF
def fetch_user_data(cpf):
    query = f"""
        SELECT ra_mat, ra_nome, ra_admissao, ra_cc, ct_desccc, ra_depto, r6_deptodesc,foto_url
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


# Página de Formulário
def formulario_page():
    st.title("Consulta de Dados do Funcionário")

    cpf = st.text_input("Insira o CPF do Funcionário")
    if st.button("Consultar"):
        data = fetch_user_data(cpf)
        if not data.empty:
            st.write("Matrícula:", data['ra_mat'].iloc[0])
            st.write("Nome:", data['ra_nome'].iloc[0])
            st.write("Data de Admissão:", data['ra_admissao'].iloc[0])
            st.write("Centro de Custo:", data['ra_cc'].iloc[0])
            st.write("Nome do Centro de Custo:", data['ct_desccc'].iloc[0])
            st.write("Departamento:", data['r6_deptodesc'].iloc[0])
            st.image(data['foto_url'].iloc[0], caption='Foto do Funcionário')
        else:
            st.write("Nenhum dado encontrado para o CPF informado.")

# Página de Termo de Contrato
def contrato_page():
    st.title("Termo de Contrato")

    # Caminho para o seu arquivo PDF
    pdf_file_path = "caminho/para/seu/termo_de_contrato.pdf"

    st.write("Por favor, leia o termo do contrato abaixo:")
    display_pdf(pdf_file_path)
    
    aceite = st.checkbox("Eu aceito os termos do contrato")

    if aceite:
        st.write("Obrigado por aceitar o termo.")


# Configuração das Páginas
page_names_to_funcs = {
    "Formulário": formulario_page,
    "Termo de Contrato": contrato_page,
}

selected_page = st.sidebar.selectbox("Selecione a página", page_names_to_funcs.keys())
page_names_to_funcs[selected_page]()