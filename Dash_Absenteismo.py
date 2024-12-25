import pandas as pd
import pyodbc  # Para conexão com SQL Server
import warnings
pd.set_option('display.max_column', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
from urllib.parse import quote_plus
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')
senha_codificada = quote_plus("Provac@2024")
engine = create_engine(f'postgresql+psycopg2://admin_provac:{senha_codificada}@192.168.0.232:5432/provac_producao')

query = """
    WITH Meses AS (
            SELECT 1 AS MesNumero, '01/2024' AS MesNome, '20240101' AS DataInicial, '20240131' AS DataFinal
            UNION SELECT 2, '02/2024', '20240201', '20240229'
            UNION SELECT 3, '03/2024', '20240301', '20240331'
            UNION SELECT 4, '04/2024', '20240401', '20240430'
            UNION SELECT 5, '05/2024', '20240501', '20240531'
            UNION SELECT 6, '06/2024', '20240601', '20240630'
            UNION SELECT 7, '07/2024', '20240701', '20240731'
            UNION SELECT 8, '08/2024', '20240801', '20240831'
            UNION SELECT 9, '09/2024', '20240901', '20240930'
            UNION SELECT 10, '10/2024', '20241001', '20241031'
            UNION SELECT 11, '11/2024', '20241101', '20241130'
            UNION SELECT 12, '12/2024', '20241201', '20241231'),
    EscalaDiaria AS (
        SELECT 
            SPJ010.PJ_TURNO,
            SPJ010.PJ_SEMANA,
            SPJ010.PJ_DIA,
            SUM(SPJ010.PJ_SAIDA1 - SPJ010.PJ_ENTRA1) +
            SUM(SPJ010.PJ_SAIDA2 - SPJ010.PJ_ENTRA2) +
            SUM(SPJ010.PJ_SAIDA3 - SPJ010.PJ_ENTRA3) +
            SUM(SPJ010.PJ_SAIDA4 - SPJ010.PJ_ENTRA4) / 60.0 AS HorasDia
        FROM 
            SPJ010
        GROUP BY 
            SPJ010.PJ_TURNO, SPJ010.PJ_SEMANA, SPJ010.PJ_DIA
    ),
    HorasAusencia AS (
        SELECT 
            PH_MAT,
            DATEPART(MONTH, PH_DATA) AS Mes,
            SUM(PH_QUANTC) AS HorasAusentes
        FROM 
            SPH010
        WHERE 
            PH_PD IN ('007', '008', '009', '010', '011', '012', '013', '014', '020')
        AND PH_ABONO = ''
            AND PH_DATA BETWEEN '20240101' AND '20241031'
        GROUP BY 
            PH_MAT, DATEPART(MONTH, PH_DATA)
    ),
    HorasAusencia_atestado AS (
        SELECT 
            PH_MAT,
            DATEPART(MONTH, PH_DATA) AS Mes,
            SUM(PH_QUANTC) AS HorasAusenciaAtestado
        FROM 
            SPH010
        WHERE 
            PH_PD IN ('007', '008', '009', '010', '011', '012', '013', '014', '020')
            AND PH_ABONO = '001'
            AND PH_DATA BETWEEN '20240101' AND '20241231'
        GROUP BY 
            PH_MAT, DATEPART(MONTH, PH_DATA)
    )
    SELECT 
        M.MesNome as mesnome,
        SRA010.RA_MAT AS matricula,
        SRA010.RA_NOME AS nome,
        SRA010.RA_CC AS centrocusto,
        dbo.CTT010.CTT_DESC01 AS [desc_cc],
        SRA010.RA_TNOTRAB AS turno,
        dbo.SR6010.R6_DESC AS [desc_turno], 
        SRA010.RA_FILIAL AS filial,
        SUM(COALESCE(ED.HorasDia, SRA010.RA_HRSDIA)) AS horasideais,
        COALESCE(HA.HorasAusentes, 0) AS horasausencia, 
        COALESCE(HA2.HorasAusenciaAtestado, 0) AS horasausenciaatestado
    FROM 
        SRA010
    CROSS JOIN Meses M
    LEFT JOIN dbo.CTT010 ON dbo.SRA010.RA_FILIAL = dbo.CTT010.CTT_FILIAL AND dbo.SRA010.RA_CC = dbo.CTT010.CTT_CUSTO
    INNER JOIN dbo.SR6010 ON dbo.SRA010.RA_TNOTRAB = dbo.SR6010.R6_TURNO
    CROSS APPLY (
        SELECT TOP (DATEDIFF(DAY, 
            CASE 
                WHEN SRA010.RA_ADMISSA > M.DataInicial THEN SRA010.RA_ADMISSA 
                ELSE M.DataInicial 
            END,
            CASE 
                WHEN SRA010.RA_DEMISSA < M.DataFinal AND SRA010.RA_DEMISSA <> '' THEN SRA010.RA_DEMISSA 
                ELSE M.DataFinal 
            END
        ) + 1)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS DiaOffset
        FROM master.dbo.spt_values
    ) AS Dias
    LEFT JOIN EscalaDiaria ED ON 
        ED.PJ_TURNO = SRA010.RA_TNOTRAB AND
        ED.PJ_SEMANA = DATEPART(WEEKDAY, DATEADD(DAY, Dias.DiaOffset, 
            CASE 
                WHEN SRA010.RA_ADMISSA > M.DataInicial THEN SRA010.RA_ADMISSA 
                ELSE M.DataInicial 
            END
        )) AND
        ED.PJ_DIA = DATEPART(WEEKDAY, DATEADD(DAY, Dias.DiaOffset, 
            CASE 
                WHEN SRA010.RA_ADMISSA > M.DataInicial THEN SRA010.RA_ADMISSA 
                ELSE M.DataInicial 
            END
        ))
    LEFT JOIN HorasAusencia HA ON 
        HA.PH_MAT = SRA010.RA_MAT AND
        HA.Mes = M.MesNumero
    LEFT JOIN HorasAusencia_atestado HA2 ON
        HA2.PH_MAT = SRA010.RA_MAT AND
        HA2.Mes = M.MesNumero
    WHERE 
        (SRA010.RA_DEMISSA = '' OR SRA010.RA_DEMISSA > M.DataInicial)
        AND (SRA010.RA_ADMISSA <= M.DataFinal)
    GROUP BY 
        M.MesNumero, 
        M.MesNome, 
        SRA010.RA_MAT, 
        SRA010.RA_NOME, 
        SRA010.RA_CC, 
        dbo.CTT010.CTT_DESC01, 
        SRA010.RA_TNOTRAB, 
        dbo.SR6010.R6_DESC,
        HA.HorasAusentes, 
        HA2.HorasAusenciaAtestado, 
        SRA010.RA_FILIAL
    ORDER BY 
        M.MesNumero, SRA010.RA_MAT;
"""

# Função para conectar ao banco de dados e executar a query
def get_data():
    #conn = st.connection("protheus_procducao")
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=192.168.0.236;DATABASE=PROTHEUS_PRODUCAO;UID=ismael.silva;PWD=w!1zayeUAM')
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = get_data()

def inserir_no_banco(data_list, engine):
    df = pd.DataFrame(data_list)
    # Definir o nome da tabela e o esquema conforme necessário
    df.to_sql('absenteismo', engine, if_exists='append',index=False, schema='kpis')
    
inserir_no_banco(df, engine)
print('Dados Inserido com Sucesso!')