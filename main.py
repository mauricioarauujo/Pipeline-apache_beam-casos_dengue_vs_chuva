import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re

from utils import *

pipeline_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options = pipeline_options)

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
        ReadFromParquet('chuvas.parquet', skip_header_lines=1)
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