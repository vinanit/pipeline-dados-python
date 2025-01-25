import json
import csv

from processamento_dados import Dados

def transformar_dados_tabela(dados, nomes_colunas):
    dados_combinados_tabela = [nomes_colunas]

    for row in dados:
        linha = []
        for coluna in nomes_colunas:
            linha.append(row.get(coluna, 'Indisponivel'))

        dados_combinados_tabela.append(linha)
    
    return dados_combinados_tabela
        
def salvando_dados(dados, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

#Extract

dados_empresaA = Dados.leitura_dados(path_json,'json')
print('Nomes colunas Empresa A:', dados_empresaA.nomes_colunas)
print('Quantidade linhas Empresa A', dados_empresaA.qnt_linhas)

dados_empresaB = Dados.leitura_dados(path_csv,'csv')
print('Nomes colunas empresa B:', dados_empresaB.nomes_colunas)
print('Quantidade Linhas empresa B:', dados_empresaB.qnt_linhas)

#Transform

key_mapping = {'Nome do Item':'Nome do Produto',
               'Classificação do Produto':'Categoria do Produto',
               'Valor em Reais (R$)':'Preço do Produto (R$)',
               'Quantidade em Estoque':'Quantidade em Estoque',
               'Nome da Loja':'Filial',
               'Data da Venda':'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)
print(dados_empresaB.nomes_colunas)

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print('Nomes colunas fusao:', dados_fusao.nomes_colunas)
print('Quantidade Linhas fusao:', dados_fusao.qnt_linhas)

#Load

path_dados_combinados = 'data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(path_dados_combinados)
