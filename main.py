import apache_beam as beam #modelo de programação para definir o ETL na pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText #realizar a leitura dos arquivos

#criando pipeline
pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

def texto_para_dicionario(item, delimitador='|'):
    return item.split(delimitador)

def lista_para_dicionario(item,colunas):
    return dict(zip(colunas,item))

def tratar_data(item):
    # recebe 28-02-2022 e quebra [28,02,2022]
    aux = item['data_iniSE'].split('-')
    # modifica [28,02,2022] para 02-2022
    item['ano_mes'] = '-'.join(aux[:2])
    return item

def chave_uf(item):
    return (item['uf'], item)

colunas_dengue = ['id','data_iniSE','casos','ibge_code','cidade','uf','cep','latitude','longitude']

#pcollections
dengue = (
    pipeline
    | "Lendo arquivo de casos de dengue" >>
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "Converter o texto em lista" >>
        beam.Map(texto_para_dicionario)
    | "Converter a lista em dicionario" >>
        beam.Map(lista_para_dicionario, colunas_dengue)
    | "Converter data_iniSE para ano_mes" >>
        beam.Map(tratar_data)
    | "Gerar tupla com estado como chave e o item" >>
        beam.Map(chave_uf)
    | "Agrupar pela chave (UF)" >>
        beam.GroupByKey()
    | "Exibir resultado" >>
        beam.Map(print)
)

pipeline.run()

