# %%
import requests
import pandas as pd
from odf.opendocument import load
from odf.table import Table, TableRow, TableCell

# URLs dos arquivos
urls = {
    "2020_FV": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/9423a65e-391c-4450-94cb-bee5b6ee9491/download/frota-de-veiculos-ufmg-2020.csv",
    "2020_FV_COU": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/dfc3e716-2153-40e9-a36a-21b7bfaebd1c/download/frota-de-veiculos-e-consumo.odt",
    "2021_GF": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/97745e48-de90-47a2-8073-77593536a155/download/relatorio-gastos-frota.csv",
    "2022_FV": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/c5cb7bc6-13cd-4536-b56e-957211bb34fa/download/total-frota-ufmg-2022.csv",
    "2022_RA": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/905e066e-57c2-42de-9e73-55deef52473a/download/relatorio-abastecimento-2022.csv",
    "2023_DV": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/b78f6c9f-afa7-423e-b846-6d8fe5b66872/download/dados-veiculos-2023.csv",
    "2023_VT": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/c1e5937b-1404-445f-92d2-454ba3a551ec/download/dados-veiculos-por-tipo-2023.csv",
    "2023_DEV": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/7fbb12aa-38b5-490e-852d-9cb1ba583ad8/download/despesas-por-veiculo-2023.csv",
    "2023_FV": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/df967747-6fac-4b17-9b30-2eb7df1a9265/download/frota-veiculos-2023.csv",
    "2023_DF": "https://dados.ufmg.br/dataset/4159e456-6b99-4f0d-af9b-3237b8904b9f/resource/fef7aadb-edbb-48af-8d5a-fb8e80d6ae68/download/dados-frota-ufmg-2023.odt",
}   

# Variáveis para armazenar os DataFrames
df_2020_FV = None
df_2020_FV_COU = None
df_2021_GF = None
df_2022_FV = None
df_2022_RA = None
df_2023_DV = None
df_2023_VT = None
df_2023_DEV = None
df_2023_FV = None
df_2023_DF = None
df_odt_1 = None
df_odt_2 = None

# Função para ler arquivo ODT e converter para DataFrame
def read_odt(url):
    response = requests.get(url)
    with open("temp.odt", 'wb') as f:
        f.write(response.content)
    
    doc = load("temp.odt")
    rows = []
    for table in doc.getElementsByType(Table):
        for row in table.getElementsByType(TableRow):
            cells = []
            for cell in row.getElementsByType(TableCell):
                text = "".join([str(p) for p in cell.childNodes])
                cells.append(text)
            rows.append(cells)
    
    header = rows[0]  # Manter a primeira linha como cabeçalho
    data = rows[1:]   # Manter todas as linhas após a primeira como dados

    #max_cols = 4  # Garantir que temos 4 colunas
    #for i, row in enumerate(data):
    #    while len(row) < max_cols:
    #        row.append("")
    #    if len(row) > max_cols:
    #        row = row[:max_cols]
    #    data[i] = row
    
    df = pd.DataFrame(data, columns=header)

    return df

# Função para dividir o DataFrame onde há linhas vazias
def split_dataframe(df):
    idx = df[(df == "").all(axis=1)].index
    if len(idx) > 0:
        idx = idx[0]
        df1 = df.iloc[:idx].reset_index(drop=True)
        df2 = df.iloc[idx+1:].reset_index(drop=True)
        return df1, df2
    return df, pd.DataFrame()  # Retornar segundo DataFrame vazio se não houver linha vazia

# Loop para ler cada arquivo e salvar em uma variável separada
for year, url in urls.items():
    if url.endswith('.csv') and year == "2020_FV":
        df_2020_FV = pd.read_csv(url, encoding='latin1', sep=';', skiprows=2, on_bad_lines='skip')
    elif url.endswith('.csv') and year == "2021_GF":
        df_2021_GF = pd.read_csv(url, encoding='latin1', sep=',', skiprows=2)
    elif url.endswith('.csv') and year == "2022_FV":
        df_2022_FV = pd.read_csv(url, encoding='latin1', sep=',')
    elif url.endswith('.csv') and year == "2022_RA":
        df_2022_RA = pd.read_csv(url, encoding='latin1', sep=',')
    elif url.endswith('.csv') and year == "2023_DV":
        df_2023_DV = pd.read_csv(url, encoding='latin1', sep=',', skiprows=2)
    elif url.endswith('.csv') and year == "2023_VT":
        df_2023_VT = pd.read_csv(url, encoding='latin1', sep=',', skiprows=2)
    elif url.endswith('.csv') and year == "2023_DEV":
        df_2023_DEV = pd.read_csv(url, encoding='latin1', sep=',', skiprows=2)
    elif url.endswith('.csv') and year == "2023_FV":
        df_2023_FV = pd.read_csv(url, encoding='latin1', sep=',', skiprows=2)
    elif url.endswith('.csv') and year == "2023_DF":
        df_2023_DF = pd.read_csv(url, encoding='latin1', sep=',', skiprows=2)
    elif url.endswith('.odt') and year == "2020_FV_COU":
        df_2020_FV_COU = read_odt(url)
        df_odt_1, df_odt_2 = split_dataframe(df_2020_FV_COU)
    elif url.endswith('.odt') and year == "2023_DF":
        df_2023_DF = read_odt(url)
#%%
# Tratamento de dados
# 2020_FV
# Ordenar as colunas
df_2020_FV = df_2020_FV[['Placa', 'nº Chassis', 'Ano Fabbricação', 'Combustível', 'Idade',
                         'Destinação', 'Tipo', 'Unidade de lotação', 'Setor lotação', 'Valor Aquisição']]
#Renomear as colunas
df_2020_FV.rename(columns={'Placa': 'PLACA', 'nº Chassis': 'CHASSI', 'Ano Fabbricação': 'ANO_FAB', 
                           'Combustível': 'COMBUSTIVEL', 'Idade': 'IDADE', 'Destinação': 'DESTINACAO', 
                           'Tipo': 'TIPO', 'Unidade de lotação': 'UNIDADE', 'Setor lotação': 'DEPARTAMENTO',
                           'Valor Aquisição': 'VLR_COMPRA'}, inplace=True)

#2021_GF
#Ordenar as colunas
df_2021_GF = df_2021_GF[['O.S.', 'VEÍCULO', 'DEPARTAMENTO', 'Data Fechamento', 'M.O Interna',
       'M.O Externa', 'Peças', 'Total']]
#Renomear as colunas
df_2021_GF.rename(columns={'O.S.':'ORDEM_SERVICO', 'VEÍCULO': 'PLACA', 'Data Fechamento': 'DT_FECHAMENTO', 
                           'M.O Interna':'MO_INTERNA', 'M.O Externa': 'MO_EXTERNA', 'Peças': 'VLR_PECA', 
                           'Total': 'VLR_TOTAL'}, inplace=True)
#Lista de valores a serem excluídos
valores_para_excluir = ['ORDEM DE SERVIÇO', 'Filtros:   Da Abert. : 01/01/2021 até  28/02/2022', 'O.S.', 'SISFROTA - GERENCIAMENTO DA FROTA  (LUCAS)']
#Excluindo as linhas onde a coluna 'A' tem qualquer um dos valores da lista
df_2021_GF = df_2021_GF[~df_2021_GF['ORDEM_SERVICO'].isin(valores_para_excluir)]
df_2021_GF = df_2021_GF[~df_2021_GF['PLACA'].isin(valores_para_excluir)]
#Removendo as ultimas linhas
df_2021_GF = df_2021_GF[:-1]

#2022_FV
#Ordenar as colunas
#Exclusão das colunas 'Unnamed: 0'(ID), 'Combustivel.1' (R), 'Aquisição.1' (R), 'Local'(SV), 'Loc.Manutenção'(SV), 'Nota de venda'(SV), 'Valor de venda'(SV)
df_2022_FV = df_2022_FV[['Placa', 'Modelo', 'Marca', 'Chassi', 'AnoFabr.', 'Combustivel', 'Aquisição',
       'Conservação', 'Unidade', 'Departamento', 'Tipo', 'Cor', 'Grupo', 'Potência', 'Cilindro', 'Preventiva', 
       'Situação', 'Patrimonio', 'Renavam',  'Pool/Dedicado/Reserva', 'Motor', 'Caixa', 'Cartão', 'Proprietario', 
       'Km Atual', 'Data Compra', 'Data Venda', 'Nota de compra', 'Valor de compra', 'Documento', 'Patrimônio', 
       'Nº Apólice']]
#Renomear as colunas
df_2022_FV.rename(columns={'Placa': 'PLACA', 'Modelo': 'MODELO', 'Marca': 'MARCA', 'Chassi': 'CHASSI', 
                           'AnoFabr.': 'ANO_FAB', 'Combustivel': 'COMBUSTIVEL', 'Aquisição': 'AQUISICAO',
                            'Conservação': 'CONSERVACAO', 'Unidade': 'UNIDADE', 'Departamento': 'DEPARTAMENTO', 
                            'Tipo': 'TIPO', 'Cor': 'COR', 'Grupo': 'GRUPO', 'Potência': 'POTENCIA', 
                            'Cilindro': 'CILINDRO', 'Preventiva': 'PREVENTIVA', 'Situação': 'SITUACAO', 
                            'Patrimonio': 'PATRIMONIO', 'Renavam': 'RENAVAM',  'Pool/Dedicado/Reserva': 'SITUACAO_ALOCACAO', 
                            'Motor': 'MOTOR', 'Caixa': 'CAIXA', 'Cartão': 'CARTAO', 'Proprietario': 'PROPRIETARIO', 
                            'Km Atual': 'KILOMETRAGEM', 'Data Compra': 'DT_COMPRA', 'Data Venda': 'DT_VENDA', 
                            'Nota de compra': 'NOTA_COMPRA', 'Valor de compra': 'VLR_COMPRA', 'Documento': 'DOCUMENTO', 
                            'Patrimônio': 'PATRIMONIO', 'Nº Apólice': 'NR_APOLICE'}, inplace=True)
#Removendo as ultimas linhas
df_2022_FV = df_2022_FV[:-3]

#2023_FV
#Ordenar as colunas
df_2023_FV = df_2023_FV[['Placa', 'Modelo', 'Marca', 'Chassi', 'AnoFabr.', 'Combustivel', 'Aquisição',
       'Conservação',  'Unidade', 'Departamento', 'Tipo']]
#Renomear as colunas
df_2023_FV.rename(columns={'Placa':'PLACA', 'Modelo': 'MODELO', 'Marca': 'MARCA', 'Chassi': 'CHASSI', 
                           'AnoFabr.': 'ANO_FAB', 'Combustivel': 'COMBUSTIVEL', 'Aquisição': 'AQUISICAO',
                           'Conservação': 'CONSERVACAO',  'Unidade': 'UNIDADE', 'Departamento': 'DEPARTAMENTO', 
                           'Tipo': 'TIPO'}, inplace=True)
#Removendo as ultimas linhas
df_2023_FV = df_2023_FV[:-1]

#2023_DV
#Ordenar as colunas
df_2023_DV = df_2023_DV[['Placa', 'Modelo', 'Marca', 'Chassi', 'AnoFabr.', 'Combustivel', 'Departamento', 'Tipo', 
                         'Cor', 'Data Compra', 'Situação', 'Km Atual', 'Renavam']]
#Renomear as colunas
df_2023_DV.rename(columns={'Placa':'PLACA', 'Modelo': 'MODELO', 'Marca': 'MARCA', 'Chassi': 'CHASSI', 
                           'AnoFabr.': 'ANO_FAB', 'Combustivel': 'COMBUSTIVEL', 'Departamento': 'DEPARTAMENTO', 
                           'Tipo': 'TIPO', 'Cor': 'COR', 'Data Compra': 'DT_COMPRA', 'Situação': 'SITUACAO', 
                           'Km Atual': 'KILOMETRAGEM', 'Renavam': 'RENAVAM'}, inplace=True)
#Removendo as ultimas linhas
df_2023_DV = df_2023_DV[:-3]

#2023_VT
#Ordenar as colunas
df_2023_VT = df_2023_VT[['Placa', 'Modelo', 'Marca', 'Chassi', 'AnoFabr.', 'Aquisição',
                         'Conservação', 'Combustivel', 'Unidade', 'Departamento', 'Tipo']]
#Renomear as colunas
df_2023_VT.rename(columns={'Placa':'PLACA', 'Modelo': 'MODELO', 'Marca': 'MARCA', 'Chassi': 'CHASSI', 
                           'AnoFabr.': 'ANO_FAB', 'Aquisição': 'AQUISICAO', 'Conservação': 'CONSERVACAO', 
                           'Combustivel': 'COMBUSTIVEL', 'Unidade': 'UNIDADE', 'Departamento': 'DEPARTAMENTO', 
                           'Tipo': 'TIPO'}, inplace=True)
#Removendo as ultimas linhas
df_2023_VT = df_2023_VT[:-1]

# %%
