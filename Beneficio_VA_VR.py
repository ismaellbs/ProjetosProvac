
print('Importando Bibliotecas')
import pandas as pd
import numpy as np
from glob import glob
import os
from datetime import datetime as dt
import datetime
import calendar
import holidays
import warnings
import win32com.client as win32
warnings.filterwarnings('ignore')
import pymssql
import math
import psycopg2
from sqlalchemy import create_engine
pd.set_option('display.max_column', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
from urllib.parse import quote_plus
senha_codificada = quote_plus("Provac@2024")
engine = create_engine(f'postgresql+psycopg2://admin_provac:{senha_codificada}@192.168.0.232:5432/provac_producao')

#Conectar ao banco do PostgreSQL
def conect_protgress():
    conn = psycopg2.connect(database = "provac_producao", 
                        user = "admin_provac", 
                        host= '192.168.0.232',
                        password = "Provac@2024",
                        port = 5432)
    cur = conn.cursor()
    return conn, cur

#Conectar ao banco do LTOTVS
def conect_totvs():
    serv = '192.168.0.236'
    databa = 'PROTHEUS_PRODUCAO'
    user = 'totvs'
    passa = 'totvsip'
    conn = pymssql.connect(serv, user, passa, databa)
    cursor = conn.cursor(as_dict=True)
    return conn, cursor

con_tot, cursor = conect_totvs()

# ---------------------------------------------------------------------------------------------------------------------------------
#Importando Ticket

print('Importando Ticket')
query = """
WITH Qry_BenVA_Geral AS(
	SELECT 
		dbo.SR0010.R0_FILIAL AS Filia, 
		dbo.SR0010.R0_MAT AS Matricula, 
		dbo.SRA010.RA_NOME AS Funcionário, 
		CONVERT(VARCHAR, CONVERT(DATE, SUBSTRING(dbo.SRA010.RA_NASC, 1, 4) + '-' + SUBSTRING(dbo.SRA010.RA_NASC, 5, 2) + '-' + SUBSTRING(dbo.SRA010.RA_NASC, 7, 2)), 103) AS Nascimento, 
		dbo.SRA010.RA_CIC AS CPF, 
		dbo.SRA010.RA_SEXO AS Sexo, 
		dbo.SRA010.RA_CC AS [Cód CC],

		dbo.CTT010.CTT_DESC01 AS [Descrição Centro de Custo],
	
		dbo.SRA010.RA_SINDICA AS [Cód Sind], 

		dbo.RCE010.RCE_DESCRI AS [Descrição Sindicato], 

		dbo.SRA010.RA_TNOTRAB AS [Cód Turno], 
		dbo.SR6010.R6_DESC AS [Descrição Turno], 
		dbo.SR0010.R0_CODIGO AS [Cód Ben], 
		dbo.RFO010.RFO_DESCR AS [Descrição do Benefício], 
		dbo.SR0010.R0_DUTILM AS [Dias Úteis], 
		dbo.SR0010.R0_QDIACAL AS [Dias Prop], 
		dbo.SR0010.R0_VLRVALE AS [Valor Vale], 
		dbo.SR0010.R0_VALCAL AS [Valor Total], 
		dbo.SR0010.R0_FERIAS AS [Dias Férias], 
		dbo.SR0010.R0_AFAST AS [Dias Afastado], 
		dbo.SR0010.R0_FALTAS AS Faltas, 
		'' AS Atestado, 
		dbo.SR0010.R0_PERIOD AS Período, 
		dbo.SR0010.R0_TPBEN AS Benefício, 
		dbo.SRA010.RA_ZZDMUN AS Município, 
		dbo.SRA010.RA_DEPTO AS [Cód Depto], 
		dbo.SQB010.QB_DESCRIC AS Departamento,
		dbo.SRA010.RA_ADMISSA AS Admissão,
		dbo.SRA010.RA_DEMISSA AS Desligamento
	FROM 
	((((((dbo.SR0010 
	INNER JOIN dbo.SRA010 ON dbo.SR0010.R0_MAT = dbo.SRA010.RA_MAT) 
	INNER JOIN dbo.CTT010 ON dbo.SRA010.RA_FILIAL = dbo.CTT010.CTT_FILIAL AND dbo.SRA010.RA_CC = dbo.CTT010.CTT_CUSTO) 
	INNER JOIN dbo.RCE010 ON dbo.SRA010.RA_SINDICA = dbo.RCE010.RCE_CODIGO) 
	INNER JOIN dbo.SR6010 ON dbo.SRA010.RA_TNOTRAB = dbo.SR6010.R6_TURNO) --dbo.SRA010.RA_FILIAL = dbo.SR6010.R6_FILIAL AND
	INNER JOIN dbo.RFO010 ON dbo.SR0010.R0_CODIGO = dbo.RFO010.RFO_CODIGO) 
	INNER JOIN dbo.SQB010 ON dbo.SRA010.RA_DEPTO = dbo.SQB010.QB_DEPTO)

	WHERE

    
		dbo.RFO010.RFO_TPVALE = '2' 
		AND dbo.SR0010.D_E_L_E_T_ <> '*' 
		AND dbo.CTT010.D_E_L_E_T_ <> '*' 
		AND dbo.RCE010.D_E_L_E_T_ <> '*' 
		AND dbo.SR6010.D_E_L_E_T_ <> '*' 
		AND dbo.RFO010.D_E_L_E_T_ <> '*' 
		AND dbo.SQB010.D_E_L_E_T_ <> '*'
		)
	SELECT 
		Qry_BenVA_Geral.Filia,
		Qry_BenVA_Geral.CPF, 
		Qry_BenVA_Geral.Funcionário AS [NOME DO USUARIO], 
		Qry_BenVA_Geral.Nascimento AS [DATA DE NASCIMENTO], 
		'Araraquaradp' AS [LOCAL], 
		'DP' AS DEPTO, 
		Qry_BenVA_Geral.[Valor Total] AS VALOR, 
		IIF(Qry_BenVA_Geral.Filia = '0101', '1796240017', IIF(Qry_BenVA_Geral.Filia = '0201', '1810040030', 'Verificar')) AS CONTRATO, 
		'1' AS Fim, 
		'Final' AS Final, 
		Qry_BenVA_Geral.Matricula, 
		Qry_BenVA_Geral.[Cód CC], 
		Qry_BenVA_Geral.[Descrição Centro de Custo], 
		Qry_BenVA_Geral.[Cód Sind], 
		Qry_BenVA_Geral.[Descrição Sindicato], 
		Qry_BenVA_Geral.[Cód Turno], 
		Qry_BenVA_Geral.[Descrição Turno], 
		Qry_BenVA_Geral.[Cód Ben], 
		Qry_BenVA_Geral.[Descrição do Benefício], 
		Qry_BenVA_Geral.[Dias Úteis], 
		Qry_BenVA_Geral.[Dias Prop], 
		Qry_BenVA_Geral.[Valor Total], 
		Qry_BenVA_Geral.[Valor Vale], 
		Qry_BenVA_Geral.[Dias Férias], 
		Qry_BenVA_Geral.[Dias Afastado], 
		Qry_BenVA_Geral.Município, 
		Qry_BenVA_Geral.Departamento,
		FORMAT(CONVERT(DATE, Qry_BenVA_Geral.Admissão), 'dd/MM/yyyy') AS Admissão, 
		--Qry_BenVA_Geral.Desligamento AS Desligamento,
		FORMAT(CONVERT(DATE, Qry_BenVA_Geral.Desligamento), 'dd/MM/yyyy') AS Desligamento,
		Qry_BenVA_Geral.Benefício
	FROM 
		Qry_BenVA_Geral
	WHERE
		Qry_BenVA_Geral.Benefício IN ('90', '92')
		AND Qry_BenVA_Geral.Desligamento = ''


	ORDER BY 
		Qry_BenVA_Geral.Matricula;
"""

ticket = pd.read_sql(query, con_tot)

ticket['Diferenca dias'] = ticket['Dias Úteis'] - ticket['Dias Prop']

# ---------------------------------------------------------------------------------------------------------------------------------

print('validando novas Escalas')
# VALIDA SE EXISTE ESCALA NAO CADASTRADA

query_turno_protheus = """ 
select 
	dbo.SRA010.RA_TNOTRAB AS [Cód Turno], 
    dbo.SR6010.R6_DESC AS descricao_turno 
from
	dbo.SRA010
INNER JOIN dbo.SR6010 ON dbo.SRA010.RA_TNOTRAB = dbo.SR6010.R6_TURNO
where
	dbo.SRA010.RA_SITFOLH <> 'D'
	AND dbo.SRA010.D_E_L_E_T_<> '*'
	AND dbo.SR6010.D_E_L_E_T_ <> '*' 
GROUP BY
	dbo.SRA010.RA_TNOTRAB,
	dbo.SR6010.R6_DESC 

ORDER BY
	dbo.SRA010.RA_TNOTRAB
	
"""

query_escala_sistem = "SELECT * FROM rh.escala_carga_horario"
escala_protheus = pd.read_sql(query_turno_protheus, con_tot)
escala_protheus['descricao_turno'] = escala_protheus['descricao_turno'].apply(lambda x: x.strip())
conn, cur = conect_protgress()
escala = pd.read_sql(query_escala_sistem, conn)
escala['horas_principais'] = pd.to_numeric(escala['horas_principais'].str.replace(',', '.').fillna(0))
escala['horas_complementar'] = pd.to_numeric(escala['horas_complementar'].str.replace(',', '.').fillna(0))
escalavalida = pd.merge(escala, escala_protheus, how='right', on='descricao_turno')
escalavalida.rename(columns={'descricao_turno':'Descricao_Turno'}, inplace=True)
escalavalida.drop_duplicates('Cód Turno', keep='first', inplace=True)
escalavalida.to_excel('Escala_vs_Sistema.xlsx', index=False)

# ---------------------------------------------------------------------------------------------------------------------------------

# Tratando atestados

print('Tratando atestados')
queryabono = """ 
SELECT 
    dbo.SPH010.PH_FILIAL AS Empresa, 
    dbo.SPH010.PH_MAT AS Matricula,
	CONVERT(DATE, dbo.SPH010.PH_DATA) AS [Data],
    SUM(dbo.SPH010.PH_QTABONO) AS SomaDePC_QTABONO, 
    dbo.SRA010.RA_TNOTRAB AS [Cód Turno],
    dbo.SR6010.R6_DESC AS Descricao_Turno
FROM 
    dbo.SPH010 
    INNER JOIN dbo.SRA010 ON dbo.SPH010.PH_MAT = dbo.SRA010.RA_MAT 
        AND dbo.SPH010.PH_FILIAL = dbo.SRA010.RA_FILIAL
    INNER JOIN dbo.SR6010 ON dbo.SRA010.RA_TNOTRAB = dbo.SR6010.R6_TURNO 
        --AND dbo.SRA010.RA_FILIAL = dbo.SR6010.R6_FILIAL
WHERE 
    dbo.SPH010.PH_ABONO NOT IN ('004', '018', '007', '011', '009', '024', '025')
	AND dbo.SPH010.PH_DATA > '20240920'
GROUP BY 
    dbo.SPH010.PH_FILIAL, 
    dbo.SPH010.PH_MAT, 
    dbo.SR6010.R6_DESC,
	dbo.SPH010.PH_DATA,
    dbo.SRA010.RA_TNOTRAB,
    dbo.SR6010.R6_DESC,
    dbo.SPH010.D_E_L_E_T_, 
    dbo.SR6010.D_E_L_E_T_
HAVING
    SUM(dbo.SPH010.PH_QTABONO) > 0
    AND dbo.SPH010.D_E_L_E_T_ <> '*' 
    AND dbo.SR6010.D_E_L_E_T_ <> '*'
ORDER BY 
    dbo.SPH010.PH_MAT; 
"""

abono = pd.read_sql(queryabono, con_tot)
abono['Descricao_Turno'] = abono['Descricao_Turno'].apply(lambda x: x.strip())

abono['Data'] = pd.to_datetime(abono['Data'], format='%Y-%m-%d')
diadasemana = {0:'Seg', 1:'Ter', 2:'Quar', 3:'Quin', 4:'Sex', 5:'Sab', 6:'Dom'}
abono['Dia_Semana'] = abono['Data'].apply(lambda x: diadasemana[x.weekday()])
abono['SomaDePC_QTABONO'] = abono['SomaDePC_QTABONO'].apply(lambda x: ((x - int(x))/0.60)+int(x)) # CONVERTER PARA DECIMAL
abono2 = pd.merge(abono, escalavalida, how='left', on='Cód Turno')
abono2['Hora Calculo'] = np.where(abono2['semana_secundario']==abono2['Dia_Semana'], abono2['horas_complementar'], abono2['horas_principais'])
abono2['Dias'] = abono2['SomaDePC_QTABONO']/abono2['Hora Calculo']
abono2['Atestados'] = np.where(abono2['Dias']<0.5, 0, 1)
atestados = abono2.groupby('Matricula', as_index=False)['Atestados'].sum()
atestados = atestados[atestados['Atestados']>0]

# ---------------------------------------------------------------------------------------------------------------------------------

# Tratando Faltas

print('Tratando Faltas')
queryFaltas = """ 
SELECT 	
	dbo.SPH010.PH_FILIAL AS Empresa, 
	dbo.SPH010.PH_MAT AS Matricula,
	CONVERT(DATE, dbo.SPH010.PH_DATA) AS [Data],
	SUM(dbo.SPH010.PH_QUANTC) AS SomaDePC_QTABONO, 
	dbo.SRA010.RA_TNOTRAB AS [Cód Turno],
	dbo.SR6010.R6_DESC AS Descricao_Turno  
from 
	SPH010 
	INNER JOIN dbo.SRA010 ON dbo.SPH010.PH_MAT = dbo.SRA010.RA_MAT 
	AND dbo.SPH010.PH_FILIAL = dbo.SRA010.RA_FILIAL
	INNER JOIN dbo.SR6010 ON dbo.SRA010.RA_TNOTRAB = dbo.SR6010.R6_TURNO
WHERE
	dbo.SPH010.PH_ABONO =''
	AND dbo.SPH010.PH_PD = '010'
	AND dbo.SPH010.D_E_L_E_T_ <> '*'
	AND dbo.SR6010.D_E_L_E_T_ <> '*'
	AND dbo.SPH010.D_E_L_E_T_ <> '*'
	AND dbo.SPH010.PH_DATA > '20240920'
GROUP BY 
	dbo.SPH010.PH_FILIAL,
	dbo.SPH010.PH_MAT,
	dbo.SPH010.PH_DATA,
	dbo.SRA010.RA_TNOTRAB,
	dbo.SR6010.R6_DESC
"""
faltas = pd.read_sql(queryFaltas, con_tot)

faltas['Descricao_Turno'] = faltas['Descricao_Turno'].apply(lambda x: x.strip())
faltas['Data'] = pd.to_datetime(faltas['Data'], format='%Y-%m-%d')
diadasemana = {0:'Seg', 1:'Ter', 2:'Quar', 3:'Quin', 4:'Sex', 5:'Sab', 6:'Dom'}
faltas['Dia_Semana'] = faltas['Data'].apply(lambda x: diadasemana[x.weekday()])
faltas['SomaDePC_QTABONO'] = faltas['SomaDePC_QTABONO'].apply(lambda x: ((x - int(x))/0.60)+int(x))
faltas2 = pd.merge(faltas, escalavalida, how='left', on='Cód Turno')
faltas2['Hora Calculo'] = np.where(faltas2['semana_secundario']==faltas2['Dia_Semana'], faltas2['horas_complementar'], faltas2['horas_principais'])
faltas2['Dias'] = faltas2['SomaDePC_QTABONO']/faltas2['Hora Calculo']
faltas2['Falta'] = np.where(faltas2['Dias']<0.5, 0, 1)
falta = faltas2.groupby('Matricula', as_index=False)['Falta'].sum()
falta = falta[falta['Falta']>0]
ticket2 = pd.merge(ticket, atestados, how='left', on=['Matricula'])
ticket2 = pd.merge(ticket2, falta, how='left', on=['Matricula'])
ticket2.fillna(0, inplace=True)

# ---------------------------------------------------------------------------------------------------------------------------------

# Admitidos

print('Tratando os Admitidos')

queryad = """
SELECT 
    dbo.SRA010.RA_MAT AS Matricula, 
    dbo.SRA010.RA_NOME AS Funcionário, 
    FORMAT(CONVERT(DATE, dbo.SRA010.RA_ADMISSA), 'dd/MM/yyyy') AS Admissão, 
    FORMAT(CONVERT(DATE, dbo.SRA010.RA_DEMISSA), 'dd/MM/yyyy') AS Desligamento, 
    dbo.SRA010.RA_TNOTRAB AS [Cód Turno], 
    dbo.SR6010.R6_DESC AS [Descrição Turno] 
FROM 
    dbo.SRA010 
INNER JOIN 
    dbo.SR6010 
    ON dbo.SRA010.RA_TNOTRAB = dbo.SR6010.R6_TURNO
WHERE 
    dbo.SRA010.RA_ADMISSA >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) -- Primeiro dia do mês anterior
    AND dbo.SRA010.RA_ADMISSA <= EOMONTH(GETDATE()) -- Último dia do mês anterior
    AND dbo.SRA010.RA_DEMISSA = ''
"""
admitidos = pd.read_sql(queryad, con_tot)
condicao2 = [(admitidos['Descrição Turno'].str.contains('25 DIAS')),
        (admitidos['Descrição Turno'].str.contains('30 DIAS')),
        ((admitidos['Descrição Turno'].str.contains('(12X36)'))&(~admitidos['Descrição Turno'].str.contains('DIAS'))),
        ((admitidos['Descrição Turno'].str.contains('FOL SAB'))&(~admitidos['Descrição Turno'].str.contains('DIAS'))),
        ((admitidos['Descrição Turno'].str.contains('FOL DOM'))&(~admitidos['Descrição Turno'].str.contains('DIAS'))),
        ((admitidos['Descrição Turno'].str.contains('(4X2)'))&(~admitidos['Descrição Turno'].str.contains('DIAS'))), 
        ((admitidos['Descrição Turno'].str.contains('SAB'))&(~admitidos['Descrição Turno'].str.contains('DIAS')))]

value2 = [1,2,3,4,5,6,7]

admitidos['condicao'] = np.select(condicao2, value2)
def admitido(data, cond):
    
    ultimo_dia = str(calendar.monthrange(dt.today().year, (dt.today().month)-1)[1])
    feriados= holidays.Brazil()    
    Quantidade_Feriado = len(feriados[datetime.date(int(dt.today().year), int(dt.today().month)-1, 1): datetime.date(int(dt.today().year), int(dt.today().month)-1, int(ultimo_dia))])
    dia_ultima = int(ultimo_dia)
    
    data2 = dt.strptime(data, '%d/%m/%Y')
    admissao_day = int(data2.day)
    
    contar_sab = 0
    cont_domingo = 0

    for d in range(admissao_day, dia_ultima+1):
                
        if datetime.date(int(dt.today().year), int(dt.today().month)-1, d).weekday()==5:
            contar_sab+=1
        elif datetime.date(int(dt.today().year), int(dt.today().month)-1, d).weekday()==6:
            cont_domingo+=1
    par = 0
    impar = 0
    
    for v in range(admissao_day, dia_ultima+1):
        if v%2 == 0:
            par +=1
        else:
            impar+=1
            
    if admissao_day%2==0:
        t = par
    else:
        t = impar
   
    condicao = [cond==1, cond==2, cond==3, cond==4, cond==5, cond==6, cond==7]
   

    value = [int(dia_ultima - admissao_day), 
             int(dia_ultima - admissao_day), 
             t, 
                int(dia_ultima - admissao_day)-contar_sab, 
                int(dia_ultima - admissao_day)-cont_domingo, 
                round(((dia_ultima - admissao_day)*20)/30,0), 
                int(dia_ultima - admissao_day)-(Quantidade_Feriado+cont_domingo)]

    valor = np.select(condicao, value, default= int(dia_ultima - admissao_day)-(contar_sab+cont_domingo+Quantidade_Feriado))
    
    return valor


valor = []
for index, valores in admitidos.iterrows():
    ad = admitido(valores['Admissão'], valores['condicao'])
    valor.append(ad)
admitidos['Qtd_admitidos'] = np.array(valor)
admitidos[admitidos['Descrição Turno']=='ESCALA 09:00-21:00 INT 01:00 (12X36)              ']
ticket2= pd.merge(ticket2, admitidos[['Matricula', 'Qtd_admitidos']], how='left', on=['Matricula'])
ticket2.fillna(0, inplace=True)
ticket2['R$ Admitidos'] = ticket2['Valor Vale'] * ticket2['Qtd_admitidos']

# ---------------------------------------------------------------------------------------------------------------------------------
# Tratamento dos Demitidos

print('Tratando os Demitidos')
demitidos = """
SELECT 
    dbo.SRA010.RA_MAT AS Matricula,
    dbo.SRA010.RA_NOME AS Funcionário, 
    FORMAT(CONVERT(DATE, dbo.SRA010.RA_ADMISSA), 'dd/MM/yyyy') AS Admissão, 
    FORMAT(CONVERT(DATE, dbo.SRA010.RA_DEMISSA), 'dd/MM/yyyy') AS Desligamento,
	dbo.SRA010.RA_TNOTRAB AS [Cód Turno], 
    dbo.SR6010.R6_DESC AS [Descrição Turno] 
FROM 
    (dbo.SRA010 INNER JOIN dbo.SR6010 ON dbo.SRA010.RA_TNOTRAB = dbo.SR6010.R6_TURNO) 
WHERE 
    dbo.SRA010.RA_DEMISSA >= DATEADD(MONTH, DATEDIFF(MONTH, 0,GETDATE()), 0) -- Primeiro dia do mês
    AND dbo.SRA010.RA_DEMISSA <= EOMONTH(GETDATE(), 0) -- Último dia do mês
"""
demitido = pd.read_sql(demitidos, con_tot)
demitido['Demitidos'] = 1
ticket2= pd.merge(ticket2, demitido[['Matricula', 'Demitidos']], how='left', on=['Matricula'])

# ---------------------------------------------------------------------------------------------------------------------------------
# Tratamento de afastados

print('Tratando os afastados')
queryafastamento_sem_retorno = """

SELECT 
    dbo.SR8010.R8_MAT AS Matricula, 
    dbo.SRA010.RA_NOME AS Funcionário, 
	dbo.RCE010.RCE_CODIGO AS [Cód Sindicato], 
    dbo.RCE010.RCE_DESCRI AS Sindicato, 
    dbo.SR8010.R8_TIPOAFA AS Cod, 
    dbo.RCM010.RCM_DESCRI AS [Descrição Afastamento], 
    FORMAT(CONVERT(DATE, dbo.SR8010.R8_DATAINI), 'dd/MM/yyyy') AS Início, 
    FORMAT(CONVERT(DATE, dbo.SR8010.R8_DATAFIM), 'dd/MM/yyyy') AS Térmimo,
	datediff(DAY, dbo.SR8010.R8_DATAINI, GETDATE()) AS [Data Afastado]

FROM 
    dbo.SR8010 
    INNER JOIN dbo.SRA010 ON dbo.SR8010.R8_MAT = dbo.SRA010.RA_MAT
    INNER JOIN dbo.RCM010 ON dbo.SR8010.R8_TIPOAFA = dbo.RCM010.RCM_TIPO
    INNER JOIN dbo.RCE010 ON dbo.SRA010.RA_SINDICA = dbo.RCE010.RCE_CODIGO
    INNER JOIN dbo.SQB010 ON dbo.SRA010.RA_DEPTO = dbo.SQB010.QB_DEPTO
    INNER JOIN dbo.CTT010 ON dbo.SRA010.RA_CC = dbo.CTT010.CTT_CUSTO AND dbo.SRA010.RA_FILIAL = dbo.CTT010.CTT_FILIAL
WHERE 

	dbo.SR8010.R8_DATAFIM =''
    AND dbo.SR8010.R8_TIPOAFA NOT IN ('001', '002', '020') 
    AND dbo.SQB010.D_E_L_E_T_ <> '*' 
    AND dbo.SR8010.D_E_L_E_T_ <> '*' 
    AND dbo.RCM010.D_E_L_E_T_ <> '*' 
    AND dbo.RCE010.D_E_L_E_T_ <> '*' 


ORDER BY
	dbo.SR8010.R8_DATAINI asc
 
"""

queryafastamento = """
SELECT 
    dbo.SR8010.R8_MAT AS Matricula, 
    dbo.SRA010.RA_NOME AS Funcionário, 
	dbo.RCE010.RCE_CODIGO AS [Cód Sindicato], 
    dbo.RCE010.RCE_DESCRI AS Sindicato, 
    dbo.SR8010.R8_TIPOAFA AS Cod, 
    dbo.RCM010.RCM_DESCRI AS [Descrição Afastamento], 
    FORMAT(CONVERT(DATE, dbo.SR8010.R8_DATAINI), 'dd/MM/yyyy') AS Início, 
    FORMAT(CONVERT(DATE, dbo.SR8010.R8_DATAFIM), 'dd/MM/yyyy') AS Térmimo,
	datediff(DAY, dbo.SR8010.R8_DATAINI, GETDATE()) AS [Data Afastado]

FROM 
    dbo.SR8010 
    INNER JOIN dbo.SRA010 ON dbo.SR8010.R8_MAT = dbo.SRA010.RA_MAT
    INNER JOIN dbo.RCM010 ON dbo.SR8010.R8_TIPOAFA = dbo.RCM010.RCM_TIPO
    INNER JOIN dbo.RCE010 ON dbo.SRA010.RA_SINDICA = dbo.RCE010.RCE_CODIGO
    INNER JOIN dbo.SQB010 ON dbo.SRA010.RA_DEPTO = dbo.SQB010.QB_DEPTO
    INNER JOIN dbo.CTT010 ON dbo.SRA010.RA_CC = dbo.CTT010.CTT_CUSTO AND dbo.SRA010.RA_FILIAL = dbo.CTT010.CTT_FILIAL
WHERE 
    dbo.SR8010.R8_TIPOAFA NOT IN ('001', '002', '020')
    and dbo.SR8010.R8_DATAFIM > GETDATE()
    AND dbo.SQB010.D_E_L_E_T_ <> '*' 
    AND dbo.SR8010.D_E_L_E_T_ <> '*' 
    AND dbo.RCM010.D_E_L_E_T_ <> '*' 
    AND dbo.RCE010.D_E_L_E_T_ <> '*'
ORDER BY
	dbo.SR8010.R8_DATAINI asc
"""

# ---------------------------------------------------------------------------------------------------------------------------------
# Importando CCT

query_cct = 'SELECT * FROM rh.cct'
cct = pd.read_sql(query_cct, conn).fillna(0)
cct.columns = ['Cód Sindicato', 'Nome', 'Desconto Falta','MATERNIDADE 120 DIAS', 'AFASTAMENTO', 'TIPO']

afastamento_semretorno = pd.read_sql(queryafastamento_sem_retorno, con_tot)
afastamento_inicio = pd.read_sql(queryafastamento, con_tot)
afastamento = pd.concat([afastamento_semretorno, afastamento_inicio])
afastamento.drop_duplicates('Matricula', keep='first', inplace=True)
afastamento2 = pd.merge(afastamento, cct[['Cód Sindicato', 'MATERNIDADE 120 DIAS', 'AFASTAMENTO', 'TIPO']], how='left', on=['Cód Sindicato'])
afastamento2['Descrição Afastamento'] = afastamento2['Descrição Afastamento'].str.strip()
afastamento2 = afastamento2[afastamento2['Descrição Afastamento']!='LICENCA REMUNERADA']
afastamento2['AFASTAMENTO'] = pd.to_numeric(afastamento2['AFASTAMENTO']).fillna(0).astype(int)
afastamento2['Data Afastado'] = pd.to_numeric(afastamento2['Data Afastado']).fillna(0).astype(int)
condicao = [((afastamento2['Descrição Afastamento']=='Aposentadoria por Invalidez') | (afastamento2['Descrição Afastamento']=='CARCERE')), 
            (((afastamento2['Descrição Afastamento']=='Afastamento Temporário por Motivo de Licença-Maternidade Pago pela Empresa') | (afastamento2['Descrição Afastamento']=='Prorrogação do Afastamento Temporário por Motivo de Licença-Maternidade')) & (afastamento2['MATERNIDADE 120 DIAS']=='SIM')), 
            afastamento2['Data Afastado'] > afastamento2['AFASTAMENTO'], 
            afastamento2['Data Afastado'] < afastamento2['AFASTAMENTO'], 
            ((afastamento2['Data Afastado']==0) & (afastamento2['AFASTAMENTO']==0))]
resultado = ['NÃO PAGA', 'PAGA', 'NÃO PAGA','PAGA', 'NÃO PAGA']
afastamento2['Pagamento'] = np.select(condicao, resultado)
ticket2 = pd.merge(ticket2, afastamento2[['Matricula', 'Pagamento']], how='left', on=['Matricula'])

# ---------------------------------------------------------------------------------------------------------------------------------
# INSERINDO PERIODOS DE PAGAMENTO


print('Inserindo os periodos de Pagamentos')


periodo = pd.read_excel('F:/dp/Benefícios/2024/Prazos Pgto Benefícios e CCT..xlsx', 'DATA PG ATUALIZADA', dtype=str, skiprows=1)
ticket2['Cód CC'] = ticket2['Cód CC'].str.strip()
ticket2 = pd.merge(ticket2, periodo[['Cód CC', 'Data de Pgto BEM']], how='left', on='Cód CC')
ticketva = ticket2[ticket2['Descrição do Benefício'].str.contains('TK VA')]
ticketcesta = ticket2[ticket2['Descrição do Benefício'].str.contains('TK CESTA')]

# ---------------------------------------------------------------------------------------------------------------------------------
# tratando VA
print('Tratando VA')
ticketva.fillna(0, inplace=True)
ticketva['Dias Calculado Corrigido'] = np.where(ticketva['Descrição do Benefício'].str.contains('DIAS'), 
                                                ticketva['Dias Prop']-ticketva['Atestados']-ticketva['Falta']-ticketva['Dias Férias'], 
                                                ticketva['Dias Prop']-ticketva['Atestados']-ticketva['Falta'])

# ---------------------------------------------------------------------------------------------------------------------------------
# Tratando Cesta
print('Tratando Cesta')
ticketcesta.fillna(0, inplace=True)
ticketcesta['Dias Calculado Corrigido'] = np.where(ticketcesta['Pagamento']=='NÃO PAGA', 0, 1)
ticketcesta.loc[ticketcesta['Qtd_admitidos']>= 15, 'R$ Admitidos'] = ticketcesta['Valor Vale']
ticketcesta.loc[ticketcesta['Qtd_admitidos']< 15, 'R$ Admitidos'] = 0

# ---------------------------------------------------------------------------------------------------------------------------------
#EXPORTANDO DADOS

print('Exportando os dados')
ticketok = pd.concat([ticketva, ticketcesta])
ticketok[ticketok['Data de Pgto BEM']=='5'].to_excel('F:/dp/Benefícios/2024/11_2024/Ticket_VA_Dia5.xlsx', index=False)
ticketok[ticketok['Data de Pgto BEM']=='1'].to_excel('F:/dp/Benefícios/2024/11_2024/Ticket_VA_Dia1.xlsx', index=False)
ticketok[ticketok['Data de Pgto BEM']=='0'].to_excel('F:/dp/Benefícios/2024/11_2024/Ticket_VASemDia.xlsx', index=False)


