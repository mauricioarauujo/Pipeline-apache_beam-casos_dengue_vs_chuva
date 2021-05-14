import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import ReadFromParquet
from apache_beam.io import WriteToParquet
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re

colunas_dengue = [
                    'id',
                    'data_iniSE',
                    'casos',
                    'ibge_code',
                    'cidade',
                    'uf',
                    'cep',
                    'latitude',
                    'longitude'
]

colunas_chuvas = [
                    'data',
                    'mm',
                    'uf'
]

def texto_para_lista(elemento, delimitador ='|'):
    """
    Função que retorna uma lista apos receber uma string

    """
    return elemento.split(delimitador)

def lista_para_dicionario(elemento, colunas):
    """
    Função que retorna o dicionario apos a entrada de uma lista
    
    """
    return dict(zip(colunas, elemento))
def trata_datas(elemento, coluna_data):
    """
    Recebe um dicionario e altera a coluna de data 
    """
    elemento['ano_mes'] = '-'.join(elemento[coluna_data].split('-')[:2])
    
    return elemento

def chave_uf(elemento):
    """
    Recebe um dicionario e retorna uma tupla (UF, dicionario)
    """
    
    chave = elemento['uf']

    return chave, elemento

def chuva_chave_uf_ano_mes_lista(elemento):
    """
    Recebe um dicionario de elementos {'data': '2015-01-31', 'mm': 0.0, 'uf': 'CE'} e retorna uma tupla contendo uma chave e o valor de chuva em mm
    ('UF-ANO-MES', '1.3')
    """

    data, mm, uf = elemento['data'], elemento['mm'], elemento['uf']
    
    if mm < 0: mm = 0

    ano_mes = '-'.join(data.split('-')[:2])
    chave = f"{uf}-{ano_mes}"
    return chave, mm


def arredonda(elemento):
    """
    Recebe uma tupla e retorna a mesma com o valor mm arredondado
    """

    chave , mm = elemento

    return chave, round(mm,1)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{},{}]) e retorna uma tupla com uf_ano_mes e casos ('RS-2014-12', 8.0)
    """
    uf, registros = elemento

    for registro in registros:
        if re.search(r'\d',registro['casos']):        
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)
def filtra_campos_nao_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    """

    chave, dados = elemento

    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    return False


def descompacta_elementos(elemento):
    """ 
    Recebe uma tupla com uma chuva e um dicionario ('CE-2015-11', {'chuvas': 0.4, 'dengue': 21.0})
    Retorna uma tupla com uf,ano,mes,mm de chuva,casos de dengue ('CE', '2015','11', '0.4','21.0') 
    """

    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]

    uf, ano, mes = chave.split('-')

    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(elemento, delimitador = ';'):
    """
    Recebe uma tupla e retorna uma string delimitada
    """

    return delimitador.join(elemento)

