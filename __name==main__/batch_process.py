import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText

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
    elemento['DT_PEDIDO_MES_ANO'] = '/'.join(elemento['DT_PEDIDO'].split('/')[1:])
    return elemento


p1 = beam.Pipeline()    

batch = (

    p1

    | 'read bucket'  >> beam.io.ReadFromText('s3://kubernets-flink-poc/televenda.txt')
    | "text to list" >> beam.Map(texto_para_lista)
    | "lista to dic" >> beam.Map(lista_para_dicionario, televenda)
    | "criar coluna" >> beam.Map(trata_datas)
    | beam.Map(print)


)

p1.run()