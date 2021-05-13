import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re






pipeline_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options = pipeline_options)


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
    Recebe uma lista de elementos ['2016-01-04', '4.2', 'TO'] e retorna uma tupla contendo uma chave e o valor de chuva em mm
    ('UF-ANO-MES', '1.3')
    """

    data, mm, uf = elemento
    mm = float(mm)

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



dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('casos_dengue.txt', skip_header_lines = 1)
    | "Dengue - texto para lista" >>
        beam.Map(texto_para_lista)
    | "Dengue - lista para dicionario" >>
        beam.Map(lista_para_dicionario, colunas_dengue)
    | "Dengue - Criar campo ano_mes" >>
        beam.Map(trata_datas, coluna_data = 'data_iniSE')
    | "Dengue - Criar chave pelo estado" >> 
        beam.Map(chave_uf)
    | "Dengue - Agrupar pelo estado" >>
        beam.GroupByKey()
    | "Dengue - Descompactar casos de dengue" >>
        beam.FlatMap(casos_dengue)
    | "Dengue - Soma dos casos pela chave" >>
        beam.CombinePerKey(sum)
    #| "Dengue - Mostrar resultados" >>   beam.Map(print)

)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >>
        ReadFromText('chuvas.csv', skip_header_lines=1)
    | "Chuvas - de texto para lista" >>
        beam.Map(texto_para_lista, delimitador = ',')
    | 'Chuvas - Criar chave uf_ano_mes' >>
         beam.Map(chuva_chave_uf_ano_mes_lista)
    | 'Chuvas - Soma dos mm pela chave' >>
        beam.CombinePerKey(sum)
    | 'Chuvas - Arredondar resultados' >>
        beam.Map(arredonda)
#    | "Chuvas - Mostrar resultados" >> beam.Map(print)

)

final = (
    ({'chuvas': chuvas, 'dengue': dengue})
    | "Final - Mesclar pcols" >> 
        beam.CoGroupByKey()
    | "Final - filtrar valores vazios" >> 
        beam.Filter(filtra_campos_nao_vazios)
    | "Final - descompacta a saída" >>
        beam.Map(descompacta_elementos)
    | "Final - preparar csv" >>
        beam.Map(preparar_csv)
    
    #| "Final - Mostrar resultados" >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

final | "Criar arquivo csv" >> WriteToText('final', file_name_suffix='.csv', header = header)

pipeline.run()