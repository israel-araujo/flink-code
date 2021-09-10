# -*- Coding: UTF-8 -*-
#coding: utf-8

import logging
import apache_beam as beam
import pyarrow as pa
from apache_beam.io.aws.s3io import S3IO
from apache_beam.io.aws.clients.s3 import messages
from apache_beam.transforms import PTransform
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.core import CombinePerKey, GroupBy, GroupByKey, Map
from apache_beam.version import __version__ as beam_version
import apache_beam.transforms.window as window
options = PipelineOptions()

televenda = ['CD_TELEVENDA', 'CD_FILIAL', 'CD_TIPO_CARTAO', 'CD_ORIGEM_TELEVENDA', 'CD_MOTIVO_TELEVENDA', 'CD_OPERADOR',
             'NR_CEP', 'NR_ROT_DIST', 'CD_BANDEIRA_CARTAO', 'CD_FORMA_PAGTO', 'CD_ENTREGADOR', 'ID_CLIENTE', 'NR_ENDERECO',
             'CD_STATUS_TELEVENDA', 'FL_CONVENIO', 'NR_PEDIDO', 'NR_SEQ_PEDIDO', 'DT_PEDIDO', 'DTHR_PEDIDO', 'QT_TOTAL',
             'QT_TOTAL_MEDICAMENTO', 'QT_TOTAL_PERFUMARIA', 'VL_TOTAL', 'VL_DESC_OFERTA', 'PC_DESC', 'VL_DESC_CARTAO',
             'VL_DESC_FAIXA1', 'VL_TOTAL_LIQUIDO', 'VL_FRETE', 'FL_FRETE_LIBERADO', 'FL_ORCAMENTO', 'VL_TOTAL_PEDIDO',
             'FL_AGENDA', 'QT_PONTOS', 'HR_ATEND_PREVISTO', 'HR_IMPRESSAO', 'DT_FATURA', 'HR_FATURA', 'HR_SAIDA_ENTREGA',
             'HR_ENTREGA', 'CD_OPERADOR_FATURA', 'VL_TROCO', 'NR_CUPOM', 'CD_BANCO_CHEQUE', 'NR_ROMANEIO_ENTREGA', 'VL_DINHEIRO',
             'HR_RETORNO_ENTREGA', 'NR_CHEQUE', 'NR_CARTAO_CREDITO', 'NM_RECEBEU', 'NR_CONTA_CORRENTE', 'NR_CPF_CHEQUE', 'NR_RG_RECEBEU',
             'MM_VALIDADE_CARTAO_CREDITO', 'AA_VALIDADE_CARTAO_CREDITO', 'FL_MANUTENCAO', 'DS_OBS', 'CD_JANELA_PDV', 'CD_JANELA_APOIO',
             'CD_TELEVENDA_PAI', 'DT_CHEQUE', 'FL_ENDERECO_NOVO', 'CD_CMC7_CHEQUE', 'NR_SEQUENCIA_CUPOM', 'NR_DEPENDENTE_IDADE', 'VL_REPASSE',
             'CD_FILIAL_ORIGEM_PEDIDO', 'QT_ROTEIRIZACAO', 'FL_AGUARDANDO_BAIXA', 'NR_CODIGO_SEGURANCA', 'CD_TIPO_TRANS_CARTAO', 'CD_OPERADOR_BAIXA',
             'NR_RELATORIO_SANGRIA', 'VL_DIFERENCA_BAIXA', 'VL_DIFERENCA_DEVOL_CLIENTE', 'DS_OBS_DIF_BAIXA', 'VL_NF_DEVOLUCAO', 'ST_RETIRA_CAIXA',
             'FL_DEVOLUCAO', 'CD_OPERADOR_SANGRIA', 'DT_SANGRIA', 'FL_ESCANEADO', 'DS_NSU', 'DT_TRANSACAO_TEF', 'DS_NSU_CANCELAMENTO', 'FL_IMPRIMIR_OFFLINE',
             'NR_AUTORIZACAO_PBM', 'CD_CANAL_INTERNET', 'NR_PEDIDO_DEVOLUCAO', 'VL_ABATIMENTO', 'VL_ARREDONDAMENTO', 'VL_GORJETA', 'VL_SUBSIDIO_EMPRESA',
             'FL_AGUARDANDO_PED_VENDA', 'VL_ABATIMENTO_VISTA', 'QT_PARCELAS', 'NR_OBJETO', 'CD_OPERADOR_MODALIDADE', 'CD_MOTIVO_ABANDONO_TELEVENDA', 'CD_CARRIER',
             'IP_OPERADOR', 'MD_DURACAO_ATENDIMENTO_S', 'NR_AGENCIA', 'NR_CPF_CNPJ_NOTA', 'DT_ENVIO_SUPERPOLO', 'CD_TELEVENDA_TURNO', 'FL_EMERGENCIAL', 'VL_CUSTO_FRETE',
             'FL_ACAO_JUDICIAL']

dept_data = [
    'id ',
    'nome ',
    'num ',
    'setor ',
    'data']


def lista_para_dicionario(elemento, colunas):
    """
    Recebe 2 listas
    Retorna 1 dicionário
    """
    return dict(zip(colunas, elemento))


def texto_para_lista(elemento, delimitador=','):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)


def trata_datas(elemento):
    """
    Recebe um dicionário e cria um novo campo com ANO-MES
    Retorna o mesmo dicionário com o novo campo
    """
    #elemento['mes-ano'] = '-'.join(elemento['data'].split('-')[1:])
    elemento['DT_PEDIDO_MES_ANO'] = '/'.join(elemento['DT_PEDIDO'].split('/')[1:])
    return elemento


def arredonda(elemento):
    """
    Recebe uma tupla ('PA-2019-06', 2364.000000000003)
    Retorna uma tupla com o valor arredondado ('PA-2019-06', 2364.0)
    """
    chave, mm = elemento
    return (chave, round(mm, 1))


def filtra_campos_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    Receber uma tupla ('CE-2015-01', {'chuvas': [85.8], 'dengue': [175.0]})
    Retorna uma tupla ('CE-2015-01', {'chuvas': [85.8], 'dengue': [175.0]})
    """
    chave, dados = elemento
    if all([
        dados['CD_FORMA_PAGTO'],
        dados['CD_BANDEIRA_CARTAO']

    ]):

        return True
    return False


p1 = beam.Pipeline()


def descompactar_elementos(elem):
    """
    Receber uma tupla {'id ': '951594MT', 'nome ': 'Hitomi', 'num ': '30', 'setor ': 'Finance', 'data': '31-01-2019', 'mes-ano': '01-2019'}
    Retornar uma tupla {'id ': '951594MT', 'nome ': 'Hitomi', 'num ': '30', 'setor ': 'Finance', 'data': '31-01-2019', 'mes-ano': '01-2019'}
    """
    chave, dados = elem
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)


def preparar_csv(elem, delimitador=';'):
    """
    Receber uma tupla ('CE', 2015, 11, 0.4, 21.0)
    Retornar uma string delimitada "CE;2015;11;0.4;21.0"
    """
    return f"{delimitador}".join(elem)



def run():
    options = PipelineOptions([
        "--runner=FlinkRunner",
        "--flink_version=1.13.0",
        "--flink_master=localhost:8081",
        "--environment_type=EXTERNAL",
        "--environment_config=localhost:50000"
    ])

with beam.Pipeline(options=options) as p:

    (p

      | 'read bucket'  >> beam.io.ReadFromText('s3://kubernets-flink-poc/televenda.txt')
      | "text to list" >> beam.Map(texto_para_lista)
      | "lista to dic" >> beam.Map(lista_para_dicionario, televenda)
      | "criar coluna" >> beam.Map(trata_datas)
    # | "Descompactar" >> beam.FlatMap(dept_data)
    # | 'mesclarpcols' >> beam.CoGroupByKey()
    # | 'Preparar csv' >> beam.Map(preparar_csv)
    # | 'descompa    ' >> beam.Map(descompactar_elementos)
    # | 'Filtrar     ' >> beam.Filter(filtra_campos_vazios)
    # | "Arrendondar " >> beam.Map(arredonda)
    # | 'filtt texto'  >> beam.Filter(filtering)
    # | 'lambda......' >> beam.Filter(lambda record: record[3] == 'Accounts')
    # | 'Strip'        >> beam.Map(str.strip)
    # | 'Strip header' >> beam.Map(lambda text: text.strip('REC/SAF/INTERNET \n'))
    # | 'Strip header' >> beam.Map(strip, chars=' Televendas \n')
    # | 'Format'       >> beam.MapTuple(lambda a, b,: '{}{}'.format(a, b,))
    # | 'Strip header' >> beam.Map(strip_header_and_newline)
    # | 'write bucket' >> beam.io.WriteToText('/home/israel/Documents/flink-code/input/dept_data.json')
    # | 'new file csv' >> beam.io.WriteToText('/home/israel/Documents/flink-code/input/Televenda3', file_name_suffix='.csv', header=dept_data)
    | beam.Map(print)


)

p1.run()
