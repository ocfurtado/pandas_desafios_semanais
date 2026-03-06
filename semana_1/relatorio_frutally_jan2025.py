# %%
import pandas as pd
import json
import re

# %% [markdown]
# ### Criação das variáveis com os dados que serão trabalhados

# %%
vendas_jan2025 = pd.read_csv('vendas_jan2025.csv', sep=';', encoding='latin-1')

with open('catalogo_produtos.json', 'r') as f:
    json_bruto = json.load(f)
catalogo_produtos = pd.json_normalize(json_bruto, sep='_')

lojas_nordeste = pd.read_excel('lojas_nordeste.xlsx', sheet_name='ativas')

# %% [markdown]
# ### Limpeza de vendas_jan2025
# #### 1 - Padronização das datas para o formato ISO
# #### 2 - Limpeza das informações desnecessárias na coluna 'preco_unitario' e conversão para float
# #### 3 - Limpeza das informações desnecessárias na coluna 'desconto_percentual', conversão para float, formatação correta para percentual e arredondamento até 3 casas decimais
# #### 4 - Identificação dos nulos existentes, para verificar se os dados podem ser derivados dos outros arquivos
# #### 5 - Padronização dos nomes nas colunas 'vendedor' e 'forma_pagamento'
# #### 6 - Eliminação das duplicatas

# %%
datas_iso = pd.to_datetime(vendas_jan2025['data_venda'], format='%Y-%m-%d', errors='coerce')
datas_br = pd.to_datetime(vendas_jan2025['data_venda'], format='%d/%m/%Y', errors='coerce')
vendas_jan2025['data_venda'] = datas_iso.fillna(datas_br)

vendas_jan2025['preco_unitario'] = (vendas_jan2025['preco_unitario']
 .str.replace(r'R\$ ', '', regex=True)
 .str.replace(',', '.')
 .str.strip()
 .astype('float') 
)

vendas_jan2025['desconto_percentual'] = ((vendas_jan2025['desconto_percentual']
 .str.replace(',', '.')
 .str.replace(r'\%', '', regex=True)
 .str.strip()
 .astype(float)
) / 100).round(3)

vendas_jan2025_nulos = ((vendas_jan2025['desconto_percentual'].isna() == True) |
                                   (vendas_jan2025['vendedor'].isna() == True) |
                                   (vendas_jan2025['forma_pagamento'].isna() == True))

vendas_jan2025['vendedor'] = (vendas_jan2025['vendedor']
 .str.replace(r'\s+', ' ', regex=True)
 .str.title()
 .str.strip())

vendas_jan2025['forma_pagamento'] = (vendas_jan2025['forma_pagamento']
 .str.replace(r'\s+', ' ', regex=True)
 .str.title()
 .str.strip())

vendas_jan2025 = vendas_jan2025.drop_duplicates()
vendas_jan2025 = vendas_jan2025.reset_index(drop=True)

# %% [markdown]
# ### Limpeza de catalogo_produtos
# #### 1 - Remoção de espaços duplos+ e de espaços iniciais/finais
# #### 2 - Transformação da coluna 'preco_custo' para o padrão numeral aceito pelo Pandas e transformação para float
# #### 3 - Padronização das classificações ltda, s.a. e me
# #### 4 - Remoção de produtos com IDs duplicadas e reset do index

# %%
catalogo_produtos['nome_produto'] = (catalogo_produtos['nome_produto']
 .str.replace(r'\s+', ' ', regex=True)
 .str.strip())

catalogo_produtos['categoria'] = (catalogo_produtos['categoria']
 .str.replace(r'\s+', ' ', regex=True)
 .str.strip())

catalogo_produtos['fornecedor_cnpj'] = (catalogo_produtos['fornecedor_cnpj']
.str.replace(r'\s+', ' ', regex=True)
 .str.strip())

catalogo_produtos['fornecedor_estado'] = (catalogo_produtos['fornecedor_estado']
 .str.replace(r'\s+', ' ', regex=True)
 .str.strip())

catalogo_produtos['preco_custo'] = (catalogo_produtos['preco_custo']
 .str.replace(r'R\$ ', '', regex=True)
 .str.replace(',', '.')
 .str.strip()
 .astype('float'))

catalogo_produtos['fornecedor_nome'] = (catalogo_produtos['fornecedor_nome']
 .str.replace(r'\bltda\b', 'LTDA', flags=re.IGNORECASE, regex=True)
 .str.replace(r'\bs\.a\.\b', 'S.A.', flags=re.IGNORECASE, regex=True)
 .str.replace(r'\bme\b', 'ME', flags=re.IGNORECASE, regex=True)
 .str.replace(r'\s+', ' ', regex=True)
 .str.strip())

catalogo_produtos = catalogo_produtos.drop_duplicates('id_produto')

catalogo_produtos = catalogo_produtos.reset_index(drop=True)

# %% [markdown]
# ### Limpeza de lojas_nordeste
# #### 1 - Ajustes nas tabelas de formato string (espaços duplos, espaços inicias/finais e camel case)
# #### 2 - Ajuste dos nulos na coluna 'metros_quadrados'
# #### 3 - Conversão das datas para o formato ISO

# %%
lojas_nordeste['nome_loja'] = (lojas_nordeste['nome_loja']
                               .str.replace(r'\s+', ' ', regex=True)
                               .str.title()
                               .str.strip())

lojas_nordeste['cidade'] = (lojas_nordeste['cidade']
                               .str.replace(r'\s+', ' ', regex=True)
                               .str.strip())

lojas_nordeste['estado'] = (lojas_nordeste['estado']
                               .str.replace(r'\s+', ' ', regex=True)
                               .str.strip())

lojas_nordeste['gerente'] = (lojas_nordeste['gerente']
 .str.replace(r'\s+', ' ', regex=True)
 .str.strip())

lojas_nordeste['metros_quadrados'] = (lojas_nordeste['metros_quadrados']
 .fillna(0))

datas_iso = pd.to_datetime(lojas_nordeste['data_inauguracao'], format='%Y-%m-%d', errors='coerce')
datas_br = pd.to_datetime(lojas_nordeste['data_inauguracao'], format='%d/%m/%Y', errors='coerce')
lojas_nordeste['data_inauguracao'] = datas_iso.fillna(datas_br)

# %% [markdown]
# ### Consolidação dos DataFrames
# #### 1 - Ao consolidar o DF, optou-se por juntá-lo com o parâmetro how='inner', na chave comum entre cada DF
# #### 1.1 - Escolheu-se esta opção para fazer a intersecção entre o que é comum a todos os DF
# #### 2 - Não foi realizado o 'dropamento' dos nulos no começo da análise, pois quer-se-ia verificar se era possível derivar os dados de outros DF
# #### 2.1 - Como não foi possível, o drop foi feito agora, para que o CSV não tivesse nulos, conforme solicitado

# %%
dados_consolidados = (vendas_jan2025.merge(catalogo_produtos, how='inner', on='id_produto')).merge(lojas_nordeste, how='inner', on='id_loja')

dados_consolidados =  dados_consolidados.dropna()

dados_consolidados = dados_consolidados.reset_index(drop=True)

# %% [markdown]
# ### Relatório Final

# %%
dados_consolidados.to_csv('relatorio_frutally_jan2025.csv', sep=',', index=False, encoding='utf-8')


