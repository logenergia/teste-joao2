"""CREATE TABLE BD_TESTE.Prev_carga_NW(
	id int auto_increment,
    mes_pmo date not null,
    mes_prev date not null,
    data_revisao date not null,
    subsis tinyint not null,
	patamar tinyint not null,
    qt_horas smallint not null,
    carga_base decimal(16,10),
    mmgd_base decimal(16,10),
	exp_cgh decimal(10,5),
    exp_eol decimal(10,5),
    exp_ufv decimal(10,5),
    exp_ute decimal(10,5),
    exp_mmgd decimal(16,10),
    primary key (id),
    unique key nao_repete_pmo (mes_pmo, mes_prev, data_revisao, subsis, patamar),
    constraint mmgd_somou_certo_1 check (exp_cgh + exp_eol + exp_ufv + exp_ute < exp_mmgd + 1),
    constraint mmgd_somou_certo_2 check (exp_cgh + exp_eol + exp_ufv + exp_ute > exp_mmgd - 1)
    );
    
    '/home/ubuntu/webhook/downloads/Previsoes_de_carga_mensal_e_por_patamar_NEWAVE/RV0_PMO_Junho_2024_carga_mensal.zip'
"""
import os
import sys
import zipfile
import datetime
import pandas as pd

from logpy import tools as tl

path = os.path.join(sys.argv[1])

# abre a planilha num dataframe
with zipfile.ZipFile(path) as z:
    planilha = z.namelist()[0]
    with z.open(planilha) as p:
        df_carga = pd.read_excel(p, engine='openpyxl')

# baseado na data da revisao, sabe os meses que vao pro bd
# 0- mes do PMO
# 1- proximo mes
# 2- proximo do proximo (so vai ser usado pra comparar quando sair a prev do PMO do mes (2))
revision_date = max(df_carga['REVISION']).to_pydatetime()
mes_r = datetime.date(revision_date.year,revision_date.month,1)
mes_0 = mes_r + datetime.timedelta(days=35)
mes_0 = datetime.date(mes_0.year,mes_0.month,1)
mes_1 = mes_0 + datetime.timedelta(days=35)
mes_1 = datetime.date(mes_1.year,mes_1.month,1)
mes_2 = mes_1 + datetime.timedelta(days=35)
mes_2 = datetime.date(mes_2.year,mes_2.month,1)

# filtra por essas datas e pelo que mais precisar
filtro_datas = df_carga['DATE'].isin([mes_0,mes_1,mes_2])
filtro_type  = df_carga['TYPE'].isin(['MEDIUM'])
df_carga = df_carga.loc[filtro_datas&filtro_type].reset_index(drop=True)

# dicionarios pra traduzir alguns campos da planilha
submercados = {
    'SUDESTE' : 1,
    'SUL'     : 2,
    'NORDESTE': 3,
    'NORTE'   : 4
}
patamares = {
    'MEDIUM': 0,
    'HIGH'  : 1,
    'LOW'   : 3,
    'MIDDLE': 2
}

# monta a query pra colocar isso no bd
query = '''
    INSERT INTO Prev_carga_NW 
    (data_revisao,mes_pmo,mes_prev,subsis,patamar,qt_horas,carga_base,mmgd_base,exp_cgh,exp_eol,exp_ufv,exp_ute,exp_mmgd)
    VALUES
'''
mes_pmo = mes_0
data_revisao = revision_date.date()

for index, linha in df_carga.iterrows():
    
    mes_prev = linha['DATE'].to_pydatetime().date()
    subsis   = submercados[linha['SOURCE']]
    patamar  = patamares[linha['TYPE']]
    qt_horas = linha['GAUGE']
    
    carga_base = linha['LOAD_sMMGD']
    mmgd_base  = linha['Base_MMGD']
    exp_cgh    = linha['Exp_CGH']
    exp_eol    = linha['Exp_EOL']
    exp_ufv    = linha['Exp_UFV']
    exp_ute    = linha['Exp_UTE']
    exp_mmgd   = linha['Exp_MMGD']
    
    query += f'''
        ('{data_revisao}','{mes_pmo}','{mes_prev}','{subsis}',{patamar},{qt_horas},{carga_base},{mmgd_base},{exp_cgh},{exp_eol},{exp_ufv},{exp_ute},{exp_mmgd}),'''
query = query.rstrip(',') + ';'
       
# guarda no BD_TESTE (provisorio)
db = tl.connection_db('BD_TESTE')
try:
    db.query(query)
    db.db_commit()
except Exception as erro:
    # ignorar erro de tentou inserir duplicado, pra continuar e executar o script ali em baixo normalmente
    if erro.args[0] == 1062:
        pass
db.db_close()

# chama script que faz a imagem e manda no telegram
os.system('python "/home/ubuntu/webhook/scripts/carga-newave-telegram.py"')