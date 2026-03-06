"""
Pipeline de Consolidação — Relatório Frutally Jan/2025
Versão: Senior / Enterprise Standard
"""

import pandas as pd
import numpy as np
import json
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s — %(message)s')
log = logging.getLogger(__name__)


# =====================================================================
# FUNÇÕES AUXILIARES
# =====================================================================

def limpar_colunas_texto(df: pd.DataFrame, colunas: list) -> pd.DataFrame:
    """Padroniza colunas de texto: remove espaços extras, aplica strip."""
    for col in colunas:
        df[col] = (df[col]
                   .str.replace(r'\s+', ' ', regex=True)
                   .str.strip())
    return df


def converter_moeda_br(serie: pd.Series) -> pd.Series:
    """Converte 'R$ 1.250,90' → 1250.90 (float). Trata milhar e decimal."""
    return (serie
            .str.replace(r'R\$\s*', '', regex=True)
            .str.replace('.', '', regex=False)   # remove separador de milhar
            .str.replace(',', '.', regex=False)   # decimal BR → decimal Python
            .str.strip()
            .astype(float))


def converter_percentual_br(serie: pd.Series) -> pd.Series:
    """Converte '15,5%' → 0.155 (float). Retorna NaN para nulos."""
    return (serie
            .str.replace(r'%', '', regex=True)
            .str.replace(',', '.', regex=False)
            .str.strip()
            .astype(float)
            .div(100)
            .round(4))


def parsear_datas_mistas(serie: pd.Series) -> pd.Series:
    """Parseia datas em formato ISO (YYYY-MM-DD) e BR (DD/MM/YYYY)."""
    iso = pd.to_datetime(serie, format='%Y-%m-%d', errors='coerce')
    br = pd.to_datetime(serie, format='%d/%m/%Y', errors='coerce')
    resultado = iso.fillna(br)
    nao_parseadas = resultado.isna().sum()
    if nao_parseadas > 0:
        log.warning(f'{nao_parseadas} datas não puderam ser parseadas.')
    return resultado


# =====================================================================
# ETAPA 1 — LEITURA DOS DADOS
# =====================================================================

vendas = pd.read_csv('vendas_jan2025.csv', sep=';', encoding='latin-1')
log.info(f'vendas: {vendas.shape[0]} registros lidos.')

with open('catalogo_produtos.json', 'r', encoding='utf-8') as f:
    catalogo = pd.json_normalize(json.load(f), sep='_')
log.info(f'catalogo: {catalogo.shape[0]} registros lidos.')

lojas = pd.read_excel('lojas_nordeste.xlsx', sheet_name='ativas')
log.info(f'lojas (ativas): {lojas.shape[0]} registros lidos.')


# =====================================================================
# ETAPA 2 — LIMPEZA: VENDAS
# =====================================================================

vendas_antes = vendas.shape[0]
vendas = vendas.drop_duplicates()
log.info(f'vendas: {vendas_antes - vendas.shape[0]} duplicatas removidas.')

vendas['data_venda'] = parsear_datas_mistas(vendas['data_venda'])
vendas['preco_unitario'] = converter_moeda_br(vendas['preco_unitario'])
vendas['desconto_percentual'] = converter_percentual_br(vendas['desconto_percentual'])

colunas_texto_vendas = ['vendedor', 'forma_pagamento']
vendas = limpar_colunas_texto(vendas, colunas_texto_vendas)
vendas['vendedor'] = vendas['vendedor'].str.title()

# PIX é acrônimo — não aplica title() cegamente em forma_pagamento
# Tratamento case-specific:
mapa_pagamento = {
    'pix': 'PIX',
    'cartão crédito': 'Cartão Crédito',
    'cartão débito': 'Cartão Débito',
    'boleto': 'Boleto',
    'dinheiro': 'Dinheiro'
}
vendas['forma_pagamento'] = (vendas['forma_pagamento']
                              .str.lower()
                              .map(mapa_pagamento)
                              .fillna(vendas['forma_pagamento']))

vendas = vendas.reset_index(drop=True)


# =====================================================================
# ETAPA 3 — LIMPEZA: CATÁLOGO
# =====================================================================

colunas_texto_catalogo = ['nome_produto', 'categoria', 'fornecedor_nome',
                           'fornecedor_cnpj', 'fornecedor_estado']
catalogo = limpar_colunas_texto(catalogo, colunas_texto_catalogo)

catalogo['preco_custo'] = converter_moeda_br(catalogo['preco_custo'])

catalogo['fornecedor_nome'] = (catalogo['fornecedor_nome']
    .str.replace(r'\bltda\b', 'LTDA', flags=re.IGNORECASE, regex=True)
    .str.replace(r'\bs\.a\.\b', 'S.A.', flags=re.IGNORECASE, regex=True)
    .str.replace(r'\bme\b', 'ME', flags=re.IGNORECASE, regex=True))

cat_antes = catalogo.shape[0]
catalogo = catalogo.drop_duplicates(subset='id_produto', keep='first')
log.info(f'catalogo: {cat_antes - catalogo.shape[0]} duplicata(s) por id_produto removida(s).')

catalogo = catalogo.reset_index(drop=True)


# =====================================================================
# ETAPA 4 — LIMPEZA: LOJAS
# =====================================================================

colunas_texto_lojas = ['nome_loja', 'cidade', 'estado', 'gerente']
lojas = limpar_colunas_texto(lojas, colunas_texto_lojas)

# Imputação com mediana — não com zero
mediana_area = lojas['metros_quadrados'].median()
nulos_area = lojas['metros_quadrados'].isna().sum()
if nulos_area > 0:
    log.info(f'lojas: {nulos_area} nulo(s) em metros_quadrados imputado(s) com mediana ({mediana_area}).')
lojas['metros_quadrados'] = lojas['metros_quadrados'].fillna(mediana_area).astype(int)

lojas['data_inauguracao'] = parsear_datas_mistas(lojas['data_inauguracao'])


# =====================================================================
# ETAPA 5 — CONSOLIDAÇÃO (MERGE)
# =====================================================================

consolidado = (vendas
               .merge(catalogo, how='inner', on='id_produto')
               .merge(lojas, how='inner', on='id_loja'))

log.info(f'consolidado pós-merge: {consolidado.shape[0]} registros.')

nulos_pre_drop = consolidado.isnull().sum().sum()
if nulos_pre_drop > 0:
    log.info(f'consolidado: {nulos_pre_drop} valores nulos encontrados — removendo linhas afetadas.')
consolidado = consolidado.dropna()
consolidado = consolidado.reset_index(drop=True)

log.info(f'consolidado final: {consolidado.shape[0]} registros, {consolidado.shape[1]} colunas.')


# =====================================================================
# ETAPA 6 — VALIDAÇÃO E EXPORTAÇÃO
# =====================================================================

assert consolidado.isnull().sum().sum() == 0, 'FALHA: Nulos remanescentes no dataset final.'
assert consolidado.duplicated().sum() == 0, 'FALHA: Duplicatas remanescentes no dataset final.'
log.info('Validação OK — zero nulos, zero duplicatas.')

consolidado.to_csv('relatorio_frutally_jan2025.csv', sep=',', index=False, encoding='utf-8')
log.info('Exportação concluída: relatorio_frutally_jan2025.csv')

