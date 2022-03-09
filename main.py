import apache_beam as beam  # modelo de programação para definir o ETL na pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText  # realizar a leitura dos arquivos

import pandas as pd

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


def filtrar_campos_vazios(item):
    chave, dados = item
    #como if dados['chuva'] is not none and dados['dengue'] is not none
    if all([
        dados['chuva'],
        dados['dengue']
    ]):
        return True
    return False


def descompactar_item(item):
    chave, dados = item

    uf, ano, mes = chave.split('-')
    chuva = dados['chuva'][0]
    dengue = dados['dengue'][0]

    #return uf, int(ano), int(mes), chuva, dengue >>> proxima etapa precisa de strings
    return uf, ano, mes, str(chuva), str(dengue)


def preparar_CSV(item):
    return f"{';'}".join(item)

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
    # | "Exibir resultado" >>
    # beam.Map(print)
)

resultado = (
    ({'chuva': chuva, 'dengue': dengue})
    | "Unir as pcollections e agruparAgrupar pela chave" >>
    beam.CoGroupByKey()
    | "Remover itens que nao tem os 2 campos" >>
    beam.Filter(filtrar_campos_vazios)
    | "Descompactar itens" >>
    beam.Map(descompactar_item)
    | "Preparar CSV de saida" >>
    beam.Map(preparar_CSV)
    # | "Exibir resultado" >>
    # beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'
resultado | "Gravar arquivo CSV" >> WriteToText('saida_tratada',file_name_suffix='.csv',header=header)

pipeline.run()

df = pd.read_csv('saida_tratada.csv', delimiter=';')
df.groupby(['UF', 'ANO'])[['CHUVA', 'DENGUE']].mean()