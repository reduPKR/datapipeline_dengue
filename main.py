import apache_beam as beam  # modelo de programação para definir o ETL na pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText  # realizar a leitura dos arquivos

# criando pipeline
pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


def texto_para_dicionario(item, delimitador='|'):
    return item.split(delimitador)


def lista_para_dicionario(item, colunas):
    return dict(zip(colunas, item))


def tratar_data_dengue(item):
    # recebe 28-02-2022 e quebra [28,02,2022]
    aux = item['data_iniSE'].split('-')
    # modifica [28,02,2022] para 02-2022
    item['ano_mes'] = '-'.join(aux[:2])
    return item


def chave_uf(item):
    return (item['uf'], item)


def extrair_casos(item):
    uf, registros = item
    for registro in registros:
        if registro['casos'] != '':
            yield f"{uf}-{registro['ano_mes']}", float(registro['casos'])
        else:
            yield f"{uf}-{registro['ano_mes']}", 0.0


def tratar_data_chuva(item):
    data, mm, uf = item
    ano_mes = '-'.join(data.split('-')[:2])

    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return f"{uf}-{ano_mes}", mm



colunas_dengue = ['id', 'data_iniSE', 'casos', 'ibge_code', 'cidade', 'uf', 'cep', 'latitude', 'longitude']

# pcollections
dengue = (
        pipeline
        | "Lendo arquivo de casos de dengue" >>
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
        | "Converter o texto casos de dengue em lista" >>
        beam.Map(texto_para_dicionario)
        | "Converter a lista em dicionario" >>
        beam.Map(lista_para_dicionario, colunas_dengue)
        | "Converter data_iniSE para ano_mes" >>
        beam.Map(tratar_data_dengue)
        | "Gerar tupla com estado como chave e o item" >>
        beam.Map(chave_uf)
        | "Agrupar pela chave (UF)" >>
        beam.GroupByKey()
        | "Descompactar casos de dengue e gera chave" >>
        beam.FlatMap(extrair_casos)  # Como usou o yield em vez de return usa o flatmap
        | "Somar casos de dengue pela chave UF-ano_mes" >>
        beam.CombinePerKey(sum)  # agrupa por chave UF-ano_mes e soma o numero de casos
        #| "Exibir resultado" >>
        #beam.Map(print)
)

chuva = (
    pipeline
    | "Lendo arquivo de chuvas" >>
    ReadFromText('chuvas.csv', skip_header_lines=1)
    | "Converter o texto de chuvas em lista" >>
    beam.Map(texto_para_dicionario, ',')
    | "Converter data para ano_mes e gerar chave UF-ano_mes" >>
        beam.Map(tratar_data_chuva)
    | "Somar dados de chuva pela chave UF-ano_mes" >>
        beam.CombinePerKey(sum)
    | "Exibir resultado" >>
    beam.Map(print)
)

pipeline.run()
